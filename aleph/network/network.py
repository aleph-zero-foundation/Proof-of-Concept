import asyncio
import logging
import pickle
import socketserver

from time import time

from aleph.data_structures import Unit, Tx
from aleph.config import N_TXS, CREATE_FREQ, LOGGER_NAME, N_RECV_SYNC
from aleph.crypto import VerifyKey


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

            if len(tx_buffer) == N_TXS or (time()-prev_put_time > CREATE_FREQ):
                prev_put_time = time()
                logger.info(f'tx_server_enqueue | Putting {len(tx_buffer)} txs on queue')
                queue.put(tx_buffer)
                tx_buffer = []

    logger = logging.getLogger(LOGGER_NAME)
    logger.info(f'tx_server_start | Starting on {listen_addr}')

    with socketserver.TCPServer(listen_addr, TCPHandler) as server:
        server.serve_forever()


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

        logger.info(f'listener_sync_no {process_id} {sync_id} | Number of syncs is {n_recv_syncs}')
        if n_recv_syncs > N_RECV_SYNC:
            logger.info(f'listener_too_many_syncs {process_id} {sync_id} | Too many syncs, rejecting {peer_addr}')
            return

        n_recv_syncs += 1
        logger.info(f'listener_establish {process_id} {sync_id} | Connection established with an unknown process')

        ex_id, ex_heights, ex_hashes = await _receive_poset_info(sync_id, process_id, process.poset.n_processes, reader, 'listener', logger)

        int_heights, int_hashes = process.poset.get_max_heights_hashes()

        await _send_poset_info(sync_id, process_id, ex_id, writer, int_heights, int_hashes, 'listener', logger)

        units_received = await _receive_units(sync_id, process_id, ex_id, reader, 'listener', logger)

        succesful = await _verify_signatures(sync_id, process_id, units_received, public_key_list, executor, 'listener', logger)
        if not succesful:
            # TODO: this should not really happen in the prototype but still, we should also close sockets here
            # Ideally this should be slightly rewritten with exceptions
            logger.error(f'listener_invalid_sign {process_id} {sync_id} | got a unit from {ex_id} with invalid signature; aborting')
            n_recv_syncs -= 1
            return

        succesful = await _add_units(sync_id, process_id, ex_id, units_received, process, 'listener', logger)
        if not succesful:
            logger.error(f'listener_not_compliant {process_id} {sync_id} | got unit from {ex_id} that does not comply to the rules; aborting')
            n_recv_syncs -= 1
            return

        await _send_units(sync_id, process_id, ex_id, int_heights, ex_heights, process, writer, 'listener', logger)


        logger.info(f'listener_succ {process_id} {sync_id} | Syncing with {ex_id} succesful')
        n_recv_syncs -= 1
        writer.close()
        await writer.wait_closed()


    host_addr = addresses[process_id]
    server = await asyncio.start_server(listen_handler, host_addr[0], host_addr[1])
    serverStarted.set()

    logger = logging.getLogger(LOGGER_NAME)
    logger.info(f'server_start {process_id} | Starting sync server on {host_addr}')

    async with server:
        await server.serve_forever()


async def sync(process, initiator_id, target_id, target_addr, public_key_list, executor):
    # TODO check if units received are in good order
    # TODO if some signature is broken, still add all units with good signatures
    logger = logging.getLogger(LOGGER_NAME)

    # new sync id
    sync_id = process.sync_id
    process.sync_id += 1

    logger.info(f'sync_establish_try {initiator_id} {sync_id} | Establishing connection to {target_id}')
    reader, writer = await asyncio.open_connection(target_addr[0], target_addr[1])
    logger.info(f'sync_establish {initiator_id} {sync_id} | Established connection to {target_id}')

    int_heights, int_hashes = process.poset.get_max_heights_hashes()

    await _send_poset_info(sync_id, initiator_id, target_id, writer, int_heights, int_hashes, 'sync', logger)

    ex_id, ex_heights, ex_hashes = await _receive_poset_info(sync_id, initiator_id, process.poset.n_processes, reader, 'sync', logger)

    await _send_units(sync_id, initiator_id, target_id, int_heights, ex_heights, process, writer, 'sync', logger)

    units_received = await _receive_units(sync_id, initiator_id, target_id, reader, 'sync', logger)

    succesful = await _verify_signatures(sync_id, initiator_id, units_received, public_key_list, executor, 'sync', logger)
    if not succesful:
        logger.info(f'sync_invalid_sign {initiator_id} {sync_id} | Got a unit from {target_id} with invalid signature; aborting')
        return

    succesful = await _add_units(sync_id, initiator_id, target_id, units_received, process, 'sync', logger)
    if not succesful:
        logger.error(f'sync_not_compliant {initiator_id} {sync_id} | Got unit from {target_id} that does not comply to the rules; aborting')
        return

    logger.info(f'sync_done {initiator_id} {sync_id} | Syncing with {target_id} succesful')

    # TODO: at some point we need to add exceptions and exception handling and make sure that the two lines below are executed no matter what happens
    writer.close()
    await writer.wait_closed()


async def _send_poset_info(sync_id, process_id, ex_id, writer, int_heights, int_hashes, mode, logger):
    logger.info(f'send_poset_{mode} {process_id} {sync_id} | sending info about forkers and heights&hashes to {ex_id}')

    data = pickle.dumps((process_id, int_heights, int_hashes))
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
    units_received = pickle.loads(data)
    n_units = len(units_received)
    logger.info(f'receive_units_done_{mode} {process_id} {sync_id} | Received {n_units} units')
    return units_received


async def _send_units(sync_id, process_id, ex_id, int_heights, ex_heights, process, writer, mode, logger):
    send_ind = [i for i, (int_height, ex_height) in enumerate(zip(int_heights, ex_heights)) if int_height > ex_height]

    logger.info(f'send_units_start_{mode} {process_id} {sync_id} | Sending units to {ex_id}')
    units_to_send = []
    for i in send_ind:
        units = process.poset.units_by_height_interval(creator_id=i, min_height=ex_heights[i]+1, max_height=int_heights[i])
        units_to_send.extend(units)
    units_to_send = process.poset.order_units_topologically(units_to_send)
    data = pickle.dumps(units_to_send)
    writer.write(str(len(data)).encode())
    logger.info(f'send_units_wait_{mode} {process_id} {sync_id} | Sending {len(data)} bytes to {ex_id}')
    writer.write(b'\n')
    writer.write(data)
    logger.info(f'send_units_sent_{mode} {process_id} {sync_id} | Sent {len(data)} bytes to {ex_id}')
    #writer.write_eof()
    await writer.drain()

    logger.info(f'send_units_done_{mode} {process_id} {sync_id} | Units sent {ex_id}')


async def _verify_signatures(sync_id, process_id, units_received, public_key_list, executor, mode, logger):
    logger.info(f'{mode} {process_id} {sync_id} | Verifying signatures')

    loop = asyncio.get_running_loop()
    # TODO check if it possible to create one tast that waits for verifying all units
    # create tasks for checking signatures of all units
    pending = [loop.run_in_executor(executor, verify_signature, unit, public_key_list) for unit in units_received]

    # check iteratively if all sigantures are valid
    while pending:
        done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        for coro in done:
            if not coro.result():
                return False

    logger.info(f'verify_sign_{mode} {process_id} {sync_id} | Signatures verified')

    return True


async def _add_units(sync_id, process_id, ex_id, units_received, process, mode, logger):
    logger.info(f'add_received_{mode} {process_id} {sync_id} | trying to add {len(units_received)} units from {ex_id} to poset')
    for unit in units_received:
        process.poset.fix_parents(unit)
        if not process.add_unit_to_poset(unit):
            logger.error(f'add_received_fail_{mode} {process_id} {sync_id} | unit {unit.short_name()} from {ex_id} was rejected')
            return False
    logger.info(f'add_received_done_{mode} {process_id} {sync_id} | units from {ex_id} were added succesfully')
    return True


def verify_signature(unit, public_key_list):
    '''Verifies signatures of the unit and all txs in it'''
    return public_key_list[unit.creator_id].verify_signature(unit.signature, unit.bytestring())
