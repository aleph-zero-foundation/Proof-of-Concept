import asyncio
import logging
import pickle
import zlib
import random
import socket
import socketserver

from time import time

from aleph.const import TXPU, CREATE_FREQ, LOGGER_NAME, N_RECV_SYNC, SEND_COMPRESSED, HOST_PORT
from aleph.data_structures import Tx
from aleph.utils import timer


def tx_listener(listen_addr, queue):
    tx_buffer = []
    prev_put_time = time()

    class TCPHandler(socketserver.BaseRequestHandler):
        def handle(self):
            nonlocal tx_buffer, prev_put_time
            logger.info(f'tx_server_establish | Connection with {self.client_address}')

            data = self.request.recv(1024)
            tx = pickle.loads(data)
            tx_buffer.append(tx)

            logger.info(f'tx_server_receive | Received from {self.client_address}')

            if len(tx_buffer) == TXPU or (time()-prev_put_time > CREATE_FREQ):
                prev_put_time = time()
                logger.info(f'tx_server_enqueue | Putting {len(tx_buffer)} txs on queue')
                queue.put(tx_buffer)
                tx_buffer = []

    logger = logging.getLogger(LOGGER_NAME)
    logger.info(f'tx_server_start | Starting on {listen_addr}')

    with socketserver.TCPServer(listen_addr, TCPHandler) as server:
        server.serve_forever()


def tx_source_gen(n_processes, tx_limit, seed):
    '''
    Produces a simple tx generator.
    :param int tx_limit: number of txs for a process to input into the system.
    :param int n_processes: number of parties.
    :param int seed: seed for random generator.
    '''

    def _tx_source(tx_receiver_address, tx_queue):
        '''
        Generates transactions in bundles of size TXPU till tx_limit is reached
        :param None tx_receiver_address: needed only for comatibility of args list with network.tx_listener
        :param queue tx_queue: queue for newly generated txs
        '''
        # ensure that batches are different
        random.seed(seed)
        with open('light_nodes_public_keys', 'r') as f:
            ln_public_keys = [line[:-1] for line in f]

        proposed = 0
        while proposed<tx_limit:
            if proposed + TXPU <= tx_limit:
                offset = TXPU
            else:
                offset = tx_limit - proposed

            txs = []
            for _ in range(offset):
                source = random.choice(ln_public_keys)
                target = random.choice(ln_public_keys)
                amount = random.randint(1, 30000)
                txs.append(Tx(source, target, amount))

            proposed += offset

            tx_queue.put(txs, block=True)

    return _tx_source


async def listener(process, process_id, addresses, public_key_list, executor, serverStarted):
    n_recv_syncs = 0

    async def listen_handler(reader, writer):
        # TODO check if units received are in good order
        # TODO if some signature is broken, and all units with good signatures that can be safely added
        nonlocal n_recv_syncs
        logger = logging.getLogger(LOGGER_NAME)

        ips = [ip for ip, _ in addresses]
        peer_addr = writer.get_extra_info('peername')
        #logger.debug('listener: assuming that addresses are different')

        # new sync id
        sync_id = process.sync_id
        process.sync_id += 1

        if peer_addr[0] not in ips:
            logger.info(f'listener_close {process_id} {sync_id} | Closing conn with {peer_addr[0]}, not in address book')
            return

        logger.info(f'listener_sync_no {process_id} {sync_id} | Number of syncs is {n_recv_syncs+1}')
        if n_recv_syncs > N_RECV_SYNC:
            logger.info(f'listener_too_many_syncs {process_id} {sync_id} | Too many syncs, rejecting {peer_addr}')
            return

        n_recv_syncs += 1
        logger.info(f'listener_establish {process_id} {sync_id} | Connection established with an unknown process')

        ex_id, ex_heights, ex_hashes = await _receive_poset_info(sync_id, process_id, process.poset.n_processes, reader, 'listener', logger)
        # we have just learned the process_id of the process on the other end, updating last_synced_with_process
        process.last_synced_with_process[ex_id] = sync_id

        int_heights, int_hashes = process.poset.get_max_heights_hashes()

        await _send_poset_info(sync_id, process_id, ex_id, writer, int_heights, int_hashes, 'listener', logger)

        units_received = await _receive_units(sync_id, process_id, ex_id, reader, 'listener', logger)

        with timer(f'{process_id} {sync_id}', 'verify_signatures'):
            succesful = _verify_signatures(sync_id, process_id, units_received, public_key_list, executor, 'listener', logger)
        if not succesful:
            # TODO: this should not really happen in the prototype but still, we should also close sockets here
            # Ideally this should be slightly rewritten with exceptions
            logger.error(f'listener_invalid_sign {process_id} {sync_id} | got a unit from {ex_id} with invalid signature; aborting')
            n_recv_syncs -= 1
            return

        with timer(f'{process_id} {sync_id}', 'add_units'):
            succesful = _add_units(sync_id, process_id, ex_id, units_received, process, 'listener', logger)
        if not succesful:
            logger.error(f'listener_not_compliant {process_id} {sync_id} | got unit from {ex_id} that does not comply to the rules; aborting')
            n_recv_syncs -= 1
            return

        await _send_units(sync_id, process_id, ex_id, int_heights, ex_heights, process, writer, 'listener', logger)


        logger.info(f'listener_succ {process_id} {sync_id} | Syncing with {ex_id} succesful')
        timer.write_summary(where=logger, groups=[f'{process_id} {sync_id}'])
        n_recv_syncs -= 1
        writer.close()
        await writer.wait_closed()


    host_ip = socket.gethostbyname(socket.gethostname())
    host_port = addresses[process_id][1]
    server = await asyncio.start_server(listen_handler, host_ip, host_port)
    serverStarted.set()

    logger = logging.getLogger(LOGGER_NAME)
    logger.info(f'server_start {process_id} | Starting sync server on {host_ip}:{host_port}')

    async with server:
        await server.serve_forever()


async def sync(process, initiator_id, target_id, target_addr, public_key_list, executor):
    # TODO check if units received are in good order
    # TODO if some signature is broken, still add all units with good signatures
    logger = logging.getLogger(LOGGER_NAME)

    # new sync id
    sync_id = process.sync_id
    process_id = process.process_id
    process.sync_id += 1
    process.last_synced_with_process[target_id] = sync_id

    logger.info(f'sync_establish_try {initiator_id} {sync_id} | Establishing connection to {target_id}')
    reader, writer = await asyncio.open_connection(target_addr[0], target_addr[1])
    logger.info(f'sync_establish {initiator_id} {sync_id} | Established connection to {target_id}')

    int_heights, int_hashes = process.poset.get_max_heights_hashes()

    await _send_poset_info(sync_id, initiator_id, target_id, writer, int_heights, int_hashes, 'sync', logger)

    ex_id, ex_heights, ex_hashes = await _receive_poset_info(sync_id, initiator_id, process.poset.n_processes, reader, 'sync', logger)

    await _send_units(sync_id, initiator_id, target_id, int_heights, ex_heights, process, writer, 'sync', logger)

    units_received = await _receive_units(sync_id, initiator_id, target_id, reader, 'sync', logger)

    with timer(f'{process_id} {sync_id}', 'verify_signatures'):
        succesful = _verify_signatures(sync_id, initiator_id, units_received, public_key_list, executor, 'sync', logger)
    if not succesful:
        logger.info(f'sync_invalid_sign {initiator_id} {sync_id} | Got a unit from {target_id} with invalid signature; aborting')
        return

    with timer(f'{process_id} {sync_id}', 'add_units'):
        succesful = _add_units(sync_id, initiator_id, target_id, units_received, process, 'sync', logger)
    if not succesful:
        logger.error(f'sync_not_compliant {initiator_id} {sync_id} | Got unit from {target_id} that does not comply to the rules; aborting')
        return

    logger.info(f'sync_done {initiator_id} {sync_id} | Syncing with {target_id} succesful')
    timer.write_summary(where=logger, groups=[f'{process_id} {sync_id}'])

    # TODO: at some point we need to add exceptions and exception handling and make sure that the two lines below are executed no matter what happens
    writer.close()
    await writer.wait_closed()


async def _send_poset_info(sync_id, process_id, ex_id, writer, int_heights, int_hashes, mode, logger):
    logger.info(f'send_poset_{mode} {process_id} {sync_id} | sending info about forkers and heights&hashes to {ex_id}')

    data = pickle.dumps((process_id, int_heights, int_hashes))
    if SEND_COMPRESSED:
        data = zlib.compress(data)
    writer.write(str(len(data)).encode())
    writer.write(b'\n')
    writer.write(data)
    await writer.drain()
    logger.info(f'send_poset_{mode} {process_id} {sync_id} | sending forkers/heights {int_heights} to {ex_id}')


async def _receive_poset_info(sync_id, process_id, n_processes, reader, mode, logger):
    logger.info(f'receive_poset_{mode} {process_id} {sync_id} | Receiving info about forkers and heights&hashes from an unknown process')
    data = await reader.readuntil()
    n_bytes = int(data[:-1])
    data = await reader.readexactly(n_bytes)
    if SEND_COMPRESSED:
        data = zlib.decompress(data)
    ex_id, ex_heights, ex_hashes = pickle.loads(data)
    assert ex_id != process_id, "It seems we are syncing with ourselves."
    assert ex_id in range(n_processes), "Incorrect process id received."
    logger.info(f'receive_poset_{mode} {process_id} {sync_id} | Got forkers/heights {ex_heights} from {ex_id}')

    return ex_id, ex_heights, ex_hashes


async def _receive_units(sync_id, process_id, ex_id, reader, mode, logger):
    logger.info(f'receive_units_start_{mode} {process_id} {sync_id} | Receiving units from {ex_id}')
    data = await reader.readuntil()
    n_bytes = int(data[:-1])
    data = await reader.readexactly(n_bytes)
    logger.info(f'receive_units_bytes_{mode} {process_id} {sync_id} | Received {n_bytes} bytes from {ex_id}')
    with timer(f'{process_id} {sync_id}', 'decompress_units'):
        if SEND_COMPRESSED:
            data = zlib.decompress(data)
    with timer(f'{process_id} {sync_id}', 'unpickle_units'):
        units_received = pickle.loads(data)
    n_units = len(units_received)
    logger.info(f'receive_units_done_{mode} {process_id} {sync_id} | Received {n_bytes} bytes and {n_units} units')
    return units_received


async def _send_units(sync_id, process_id, ex_id, int_heights, ex_heights, process, writer, mode, logger):
    send_ind = [i for i, (int_height, ex_height) in enumerate(zip(int_heights, ex_heights)) if int_height > ex_height]

    logger.info(f'send_units_start_{mode} {process_id} {sync_id} | Sending units to {ex_id}')
    units_to_send = []
    for i in send_ind:
        units = process.poset.units_by_height_interval(creator_id=i, min_height=ex_heights[i]+1, max_height=int_heights[i])
        units_to_send.extend(units)
    units_to_send = process.poset.order_units_topologically(units_to_send)
    with timer(f'{process_id} {sync_id}', 'pickle_units'):
        data = pickle.dumps(units_to_send)

    with timer(f'{process_id} {sync_id}', 'compress_units'):
        if SEND_COMPRESSED:
            initial_len = len(data)
            data = zlib.compress(data)
            compressed_len = len(data)
            gained = 1.0 - compressed_len/initial_len
            logger.info(f'compression_rate {process_id} {sync_id} | Compressed {initial_len} to {compressed_len}, gained {gained:.4f} of size.')

    writer.write(str(len(data)).encode())
    logger.info(f'send_units_wait_{mode} {process_id} {sync_id} | Sending {len(units_to_send)} units and {len(data)} bytes to {ex_id}')
    writer.write(b'\n')
    writer.write(data)
    logger.info(f'send_units_sent_{mode} {process_id} {sync_id} | Sent {len(units_to_send)} units and {len(data)} bytes to {ex_id}')
    #writer.write_eof()
    await writer.drain()

    logger.info(f'send_units_done_{mode} {process_id} {sync_id} | Units sent {ex_id}')


def _verify_signatures(sync_id, process_id, units_received, public_key_list, executor, mode, logger):
    logger.info(f'{mode} {process_id} {sync_id} | Verifying signatures')

    for unit in units_received:
        if not verify_signature(unit, public_key_list):
            return False

    logger.info(f'verify_sign_{mode} {process_id} {sync_id} | Signatures verified')
    return True


def _add_units(sync_id, process_id, ex_id, units_received, process, mode, logger):
    logger.info(f'add_received_{mode} {process_id} {sync_id} | trying to add {len(units_received)} units from {ex_id} to poset')
    printable_unit_hashes = ''
    for unit in units_received:
        process.poset.fix_parents(unit)
        printable_unit_hashes += (' ' + unit.short_name())
        #logger.info(f'add_foreign {process_id} {sync_id} | trying to add {unit.short_name()} from {ex_id} to poset')
        if not process.add_unit_to_poset(unit):
            logger.error(f'add_received_fail_{mode} {process_id} {sync_id} | unit {unit.short_name()} from {ex_id} was rejected')
            return False
    logger.info(f'add_received_done_{mode} {process_id} {sync_id} | units from {ex_id} were added succesfully {printable_unit_hashes} ')
    return True


def verify_signature(unit, public_key_list):
    '''Verifies signatures of the unit and all txs in it'''
    return public_key_list[unit.creator_id].verify_signature(unit.signature, unit.bytestring())
