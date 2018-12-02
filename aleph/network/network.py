import asyncio
import marshal
import logging

from aleph.data_structures import Unit
from aleph.config import *

async def listener(poset, process_id, addresses):
    n_recv_syncs = 0

    async def listen_handler(reader, writer):
        nonlocal n_recv_syncs
        logger = logging.getLogger(LOGGING_FILENAME)

        ips = [ip for ip, _ in addresses]
        peer_addr = writer.get_extra_info('peername')
        logger.debug('listener: assuming that addresses are different')

        if peer_addr[0] not in ips:
            logger.info(f'Closing connection with {peer_addr[0]}, it is not in address book')
            return

        if n_recv_syncs > N_RECV_SYNC:
            logger.info(f'Too many synchronizations, rejecting {peer_addr}')
            return

        n_recv_syncs += 1
        logger.info(f'listener: connection established with an unknown process')

        logger.info(f'listener: receiving info about forkers and heights&hashes from an unknown process')
        data = await reader.readuntil()
        n_bytes = int(data[:-1])
        data = await reader.read(n_bytes)
        ex_id, ex_heights, ex_hashes = marshal.loads(data)
        assert ex_id != process_id, "It seems we are syncing with ourselves."
        assert ex_id in range(poset.n_processes), "Incorrect process id received."
        logger.info(f'listener: got forkers/heights {ex_heights} from {ex_id}')

        int_heights, int_hashes = poset.get_max_heights_hashes()

        logger.info(f'listener: sending info about forkers and heights&hashes to {ex_id}')

        data = marshal.dumps((process_id, int_heights, int_hashes))
        writer.write(str(len(data)).encode())
        writer.write(b'\n')
        writer.write(data)
        await writer.drain()
        logger.info(f'listener: sending forkers/heights {int_heights} to {ex_id}')

        # receive units
        logger.info(f'listener: receiving units from {ex_id}')
        data = await reader.readuntil()
        n_bytes = int(data[:-1])
        data = await reader.read(n_bytes)
        units_recieved = marshal.loads(data)
        logger.info('listener: received units')

        logger.info(f'listener: trying to add {len(units_recieved)} units from {ex_id} to poset')
        for unit in units_recieved:
            parents = [poset.unit_by_hash(parent_hash) for parent_hash in unit['parents_hashes']]
            U = Unit(unit['creator_id'], parents, unit['txs'], unit['signature'], unit['coinshares'])
            if poset.check_compliance(U):
                poset.add_unit(U)
            else:
                logger.info(f'listener: got unit from {ex_id} that does not comply to the rules; aborting')
                n_recv_syncs -= 1
                return
        logger.info(f'listener: units from {ex_id} are added succesful')

        send_ind = [i for i, (int_height, ex_height) in enumerate(zip(int_heights, ex_heights)) if int_height > ex_height]

        # send units
        logger.info(f'listener: sending units to {ex_id}')
        units_to_send = []
        for i in send_ind:
            units = poset.units_by_height(creator_id=i, min_height=ex_heights[i]+1, max_height=int_heights[i])
            units = [unit_to_dict(U) for U in units]
            units_to_send.extend(units)
        data = marshal.dumps(units_to_send)
        writer.write(str(len(data)).encode())
        writer.write(b'\n')
        writer.write(data)
        await writer.drain()
        logger.info(f'listener: units sent to {ex_id}')

        logger.info(f'listener: syncing with {ex_id} completed succesfully')
        n_recv_syncs -= 1


    host_addr = addresses[process_id]
    server = await asyncio.start_server(listen_handler, host_addr[0], host_addr[1])

    logger = logging.getLogger(LOGGING_FILENAME)
    logger.info(f'Serving on {host_addr}')

    async with server:
        await server.serve_forever()



async def sync(poset, initiator_id, target_id, target_addr):
    logger = logging.getLogger(LOGGING_FILENAME)

    logger.info(f'sync: establishing connection to {target_id}')
    reader, writer = await asyncio.open_connection(target_addr[0], target_addr[1])
    logger.info(f'sync: established connection to {target_id}')

    int_heights, int_hashes = poset.get_max_heights_hashes()

    logger.info(f'sync: sending info about own process_id and forkers/heights/hashes to {target_id}')
    #print(int_heights, int_hashes)
    data = marshal.dumps((initiator_id, int_heights, int_hashes))
    writer.write(str(len(data)).encode())
    writer.write(b'\n')
    writer.write(data)
    await writer.drain()
    logger.info(f'sync: sent own process_id forkers/heights/hashes {int_heights} to {target_id}')


    logger.info(f'sync: receiving info about target identity and forkers/heights/hashes from {target_id}')
    data = await reader.readuntil()
    n_bytes = int(data[:-1])
    data = await reader.read(n_bytes)
    ex_id, ex_heights, ex_hashes = marshal.loads(data)
    assert ex_id == target_id, "The process_id sent by target does not much the intented target_id"
    logger.info(f'sync: got target identity and forkers/heights/hashes {ex_heights} from {target_id}')

    # send units
    send_ind = [i for i, (int_height, ex_height) in enumerate(zip(int_heights, ex_heights)) if int_height > ex_height]
    logger.info(f'sync: sending units to {target_id}')
    units_to_send = []
    for i in send_ind:
        units = poset.units_by_height(creator_id=i, min_height=ex_heights[i]+1, max_height=int_heights[i])
        units = [unit_to_dict(U) for U in units]
        units_to_send.extend(units)
    data = marshal.dumps(units_to_send)
    writer.write(str(len(data)).encode())
    writer.write(b'\n')
    writer.write(data)
    await writer.drain()
    logger.info(f'sync: units sent to {target_id}')

    # receive units
    logger.info(f'sync: receiving units from {target_id}')
    data = await reader.readuntil()
    n_bytes = int(data[:-1])
    data = await reader.read(n_bytes)
    units_recieved = marshal.loads(data)
    logger.info('sync: received units')

    logger.info(f'sync: trying to add {len(units_recieved)} units from {target_id} to poset')
    for unit in units_recieved:
        parents = [poset.unit_by_hash(parent_hash) for parent_hash in unit['parents_hashes']]
        U = Unit(unit['creator_id'], parents, unit['txs'], unit['signature'], unit['coinshares'])
        if poset.check_compliance(U):
            poset.add_unit(U)
        else:
            logger.info(f'sync: got unit from {target_id} that does not comply to the rules; aborting')
            return
    logger.info(f'sync: units from {target_id} added succesfully')


    logger.info(f'sync: syncing with {target_id} completed succesfully')



async def connecter(poset, peer_addr):
    int_heights, int_hashes = poset.get_max_heights_hashes()

    reader, writer = await asyncio.open_connection(peer_addr[0], peer_addr[1])
    print('connecter: connection established')

    print('connecter: writing hh')
    data = marshal.dumps((int_heights, int_heights))
    print('connecter: hh n_bytes', len(data))
    writer.write(str(len(data)).encode())
    writer.write(b'\n')
    writer.write(data)
    await writer.drain()
    #writer.write_eof()
    print('connecter: wrote hh')

    print('connecter: reading hh')
    data = await reader.readuntil()
    n_bytes = int(data[:-1])
    print('connecter: hh reading n_bytes', n_bytes)
    data = await reader.read(n_bytes)
    ex_heights, ex_hashes = marshal.loads(data)
    print('connecter: got hh')

    # send units
    print('connecter: sending units')
    send_ind = [i for i, (int_height, ex_height) in enumerate(zip(int_heights, ex_heights)) if int_height > ex_height]
    units_to_send = []
    for i in send_ind:
        units = poset.units_by_height(creator_id=i, min_height=ex_heights[i]+1, max_height=int_heights[i])
        units = [unit_to_dict(U) for U in units]
        units_to_send.extend(units)
    data = marshal.dumps(units_to_send)
    print('connecter: sending units n_bytes', len(data))
    writer.write(str(len(data)).encode())
    writer.write(b'\n')
    writer.write(data)
    await writer.drain()
    #writer.write_eof()
    print('connecter: units send')

    # receive units
    print('connecter: receiving units')
    data = await reader.readuntil()
    n_bytes = int(data[:-1])
    print('connecter: receiving units n_bytes', n_bytes)
    data = await reader.read(n_bytes)
    units_recieved = marshal.loads(data)
    print('connecter: units received')

    print('connecter: adding units to poset', len(units_recieved))
    for unit in units_recieved:
        parents = [poset.unit_by_hash(parent_hash) for parent_hash in unit['parents_hashes']]
        U = Unit(unit['creator_id'], parents, unit['txs'], unit['signature'], unit['coinshares'])
        if poset.check_compliance(U):
            poset.add_unit(U)
        else:
            logger.info(f'sync: got unit from {pid} that does not comply to the rules; aborting')
            return
    print('connecter: units added')

    print('connecter: job complete')


def unit_to_dict(U):
    parents_hashes = [parent.hash() for parent in U.parents]
    return {'creator_id': U.creator_id,
            'parents_hashes': parents_hashes,
            'txs': U.txs,
            'signature': U.signature,
            'coinshares': U.coinshares}
