import asyncio
import logging
import marshal
import socketserver

from time import time

from aleph.data_structures import Unit, unit_to_message, Tx
from aleph.config import *
from aleph.crypto.keys import VerifyKey


def tx_listener(listen_addr, queue):
    tx_buffer = []
    prev_put_time = time()

    class TCPHandler(socketserver.BaseRequestHandler):
        def handle(self):
            nonlocal tx_buffer, prev_put_time
            logger.info(f'tx server: established connection with {self.client_address}')

            data = self.request.recv(1024)
            tx_dict = marshal.loads(data)
            tx = Tx.from_dict(tx_dict)
            tx_buffer.append(tx)

            logger.info(f'tx server: tx received from {self.client_address}')

            if len(tx_buffer) == N_TXS or (time()-prev_put_time > CREATE_FREQ):
                prev_put_time = time()
                logger.info(f'tx server: putting {len(tx_buffer)} txs on queue')
                queue.put(tx_buffer)
                tx_buffer = []

    logger = logging.getLogger(LOGGING_FILENAME)
    logger.info(f'Starting tx server on {listen_addr}')

    with socketserver.TCPServer(listen_addr, TCPHandler) as server:
        server.serve_forever()


async def listener(process, process_id, addresses, public_key_list, executor):
    n_recv_syncs = 0

    async def listen_handler(reader, writer):
        # TODO check if units received are in good order
        # TODO if some signature is broken, and all units with good signatures that can be safely added
        nonlocal n_recv_syncs
        logger = logging.getLogger(LOGGING_FILENAME)

        ips = [ip for ip, _ in addresses]
        peer_addr = writer.get_extra_info('peername')
        #logger.debug('listener: assuming that addresses are different')

        if peer_addr[0] not in ips:
            logger.info(f'listener closing connection with {peer_addr[0]}, it is not in address book')
            return

        logger.info(f'listener {process_id} current number of syncs is {n_recv_syncs}')
        if n_recv_syncs > N_RECV_SYNC:
            logger.info(f'listener too many synchronizations, rejecting {peer_addr}')
            return

        n_recv_syncs += 1
        logger.info(f'listener {process_id}: connection established with an unknown process')

        ex_id, ex_heights, ex_hashes = await _receive_poset_info(process_id, process.poset.n_processes, reader, 'listener', logger)
        int_heights, int_hashes = process.poset.get_max_heights_hashes()

        await _send_poset_info(process_id, ex_id, writer, int_heights, int_hashes, 'listener', logger)

        units_received = await _receive_units(process_id, ex_id, reader, 'listener', logger)

        succesful = await _verify_signatures(process_id, units_received, public_key_list, executor, 'listener', logger)
        if not succesful:
            logger.info(f'listener {process_id}: got a unit from {ex_id} with invalid signature; aborting')
            n_recv_syncs -= 1
            return

        succesful = await _add_units(process_id, ex_id, units_received, process, 'listener', logger)
        if not succesful:
            logger.error(f'listener {process_id}: got unit from {ex_id} that does not comply to the rules; aborting')
            n_recv_syncs -= 1
            return

        await _send_units(process_id, ex_id, int_heights, ex_heights, process, writer, 'listener', logger)


        logger.info(f'listener {process_id}: syncing with {ex_id} completed succesfully')
        n_recv_syncs -= 1
        writer.close()
        await writer.wait_closed()


    host_addr = addresses[process_id]
    server = await asyncio.start_server(listen_handler, host_addr[0], host_addr[1])

    logger = logging.getLogger(LOGGING_FILENAME)
    logger.info(f'Starting sync server on {host_addr}')

    async with server:
        await server.serve_forever()


async def sync(process, initiator_id, target_id, target_addr, public_key_list, executor):
    # TODO check if units received are in good order
    # TODO if some signature is broken, and all units with good signatures that can be safely added
    logger = logging.getLogger(LOGGING_FILENAME)

    logger.info(f'sync {initiator_id} -> {target_id}: establishing connection to {target_id}')
    reader, writer = await asyncio.open_connection(target_addr[0], target_addr[1])
    logger.info(f'sync {initiator_id} -> {target_id}: established connection to {target_id}')

    int_heights, int_hashes = process.poset.get_max_heights_hashes()

    await _send_poset_info(initiator_id, target_id, writer, int_heights, int_hashes, 'sync', logger)

    ex_id, ex_heights, ex_hashes = await _receive_poset_info(initiator_id, process.poset.n_processes, reader, 'sync', logger)

    await _send_units(initiator_id, target_id, int_heights, ex_heights, process, writer, 'sync', logger)


    units_received = await _receive_units(initiator_id, target_id, reader, 'sync', logger)

    succesful = await _verify_signatures(initiator_id, units_received, public_key_list, executor, 'sync', logger)
    if not succesful:
        logger.info(f'sync {initiator_id}: got a unit from {target_id} with invalid signature; aborting')
        return

    succesful = await _add_units(initiator_id, target_id, units_received, process, 'sync', logger)
    if not succesful:
        logger.error(f'sync {initiator_id}: got unit from {target_id} that does not comply to the rules; aborting')
        return

    logger.info(f'sync {initiator_id} -> {target_id}: syncing with {target_id} completed succesfully')

    # TODO: at some point we need to add exceptions and exception handling and make sure that the two lines below are executed no matter what happens
    writer.close()
    await writer.wait_closed()


async def _send_poset_info(process_id, ex_id, writer, int_heights, int_hashes, mode, logger):
    logger.info(f'{mode} {process_id}: sending info about forkers and heights&hashes to {ex_id}')

    data = marshal.dumps((process_id, int_heights, int_hashes))
    writer.write(str(len(data)).encode())
    writer.write(b'\n')
    writer.write(data)
    await writer.drain()
    logger.info(f'{mode} {process_id}: sending forkers/heights {int_heights} to {ex_id}')


async def _receive_poset_info(process_id, n_processes, reader, mode, logger):
    logger.info(f'{mode} {process_id}: receiving info about forkers and heights&hashes from an unknown process')
    data = await reader.readuntil()
    n_bytes = int(data[:-1])
    data = await reader.readexactly(n_bytes)
    ex_id, ex_heights, ex_hashes = marshal.loads(data)
    assert ex_id != process_id, "It seems we are syncing with ourselves."
    assert ex_id in range(n_processes), "Incorrect process id received."
    logger.info(f'{mode} {process_id}: got forkers/heights {ex_heights} from {ex_id}')

    return ex_id, ex_heights, ex_hashes


async def _receive_units(process_id, ex_id, reader, mode, logger):
    logger.info(f'{mode} {process_id}: receiving units from {ex_id}')
    data = await reader.readuntil()
    n_bytes = int(data[:-1])
    logger.info(f'{mode} {process_id}: received {n_bytes} bytes from {ex_id}')
    data = await reader.readexactly(n_bytes)
    units_received = marshal.loads(data)
    logger.info(f'{mode}, {process_id}: received units')
    return units_received


async def _send_units(process_id, ex_id, int_heights, ex_heights, process, writer, mode, logger):
    send_ind = [i for i, (int_height, ex_height) in enumerate(zip(int_heights, ex_heights)) if int_height > ex_height]

    logger.info(f'{mode} {process_id}: sending units to {ex_id}')
    units_to_send = []
    for i in send_ind:
        units = process.poset.units_by_height_interval(creator_id=i, min_height=ex_heights[i]+1, max_height=int_heights[i])
        units_to_send.extend(units)
    units_to_send = process.poset.order_units_topologically(units_to_send)
    units_to_send = [unit_to_dict(U) for U in units_to_send]

    data = marshal.dumps(units_to_send)
    writer.write(str(len(data)).encode())
    logger.info(f'{mode} {process_id}: sending {len(data)} bytes to {ex_id}')
    writer.write(b'\n')
    writer.write(data)
    #writer.write_eof()
    await writer.drain()

    logger.info(f'{mode} {process_id}: units sent to {ex_id}')


async def _verify_signatures(process_id, units_received, public_key_list, executor, mode, logger):
    logger.info(f'{mode} {process_id}: verifying signatures')

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

    logger.info(f'{mode} {process_id}: signatures verified')

    return True


async def _add_units(process_id, ex_id, units_received, process, mode, logger):
    logger.info(f'{mode} {process_id}: trying to add {len(units_received)} units from {ex_id} to poset')
    for unit in units_received:
        assert all(U_hash in process.poset.units.keys() for U_hash in unit['parents_hashes'])
        parents = [process.poset.unit_by_hash(parent_hash) for parent_hash in unit['parents_hashes']]
        txs = [Tx.from_dict(dict_tx) for dict_tx in unit['txs']]
        U = Unit(unit['creator_id'], parents, txs, unit['signature'], unit['coinshares'])
        if not process.add_unit_to_poset(U):
            return False
    logger.info(f'{mode} {process_id}: units from {ex_id} were added succesfully')
    return True


def verify_signature(unit, public_key_list):
    '''Verifies signatures of the unit and all txs in it'''
    # verify unit signature
    message = unit_to_message(unit['creator_id'], unit['parents_hashes'], unit['txs'], unit['coinshares'])
    if not public_key_list[unit['creator_id']].verify_signature(unit['signature'], message):
        return False

    # verify signatures of txs
    for tx_dict in unit['txs']:
        tx = Tx.from_dict(tx_dict)
        message = tx.to_message()
        pk = VerifyKey.from_hex(tx.issuer)
        if not pk.verify_signature(tx.signature, message):
            return False

    return True


def unit_to_dict(U):
    parents_hashes = [parent.hash() for parent in U.parents]
    return {'creator_id': U.creator_id,
            'parents_hashes': parents_hashes,
            'txs': [tx.to_dict() for tx in U.txs],
            'signature': U.signature,
            'coinshares': U.coinshares}
