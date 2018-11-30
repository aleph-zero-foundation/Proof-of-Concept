import asyncio
import marshal
import logging

from aleph.data_structures import Unit
from aleph.config import *

async def listener(poset, host_addr, addresses):
    n_recv_syncs = 0

    async def listen_handler(reader, writer):
        nonlocal n_recv_syncs
        logger = logging.getLogger(LOGGING_FILENAME)

        ips = [ip for ip, _ in addresses]
        peer_addr = writer.get_extra_info('peername')
        logger.debug('listener: assuming that addresses are different')
        pid = ips.index(peer_addr[0])

        if peer_addr[0] not in ips:
            logger.info(f'Closing connection with {peer_addr[0]}, it is not in address book')
            return

        if n_recv_syncs > N_RECV_SYNC:
            logger.info(f'Too many synchronizations, rejecting {peer_addr}')
            return

        n_recv_syncs += 1
        logger.info(f'listener: connection established with process {pid}')

        logger.info(f'listener: receiving info about forkers and heights&hashes from {pid}')
        data = await reader.readuntil()
        n_bytes = int(data[:-1])
        data = await reader.read(n_bytes)
        ex_heights, ex_hashes = marshal.loads(data)
        logger.info(f'listener: got forkers/heights {ex_heights} from {pid}')

        int_heights, int_hashes = poset.get_max_heights_hashes()

        logger.info(f'listener: sending info about forkers and heights&hashes to {pid}')
        data = marshal.dumps((int_heights, int_heights))
        writer.write(str(len(data)).encode())
        writer.write(b'\n')
        writer.write(data)
        await writer.drain()
        logger.info(f'listener: sending forkers/heights {int_heights} to {pid}')

        # receive units
        logger.info(f'listener: receiving units from {pid}')
        data = await reader.readuntil()
        n_bytes = int(data[:-1])
        data = await reader.read(n_bytes)
        units_recieved = marshal.loads(data)
        logger.info('listener: received units')

        logger.info(f'listener: trying to add {len(units_recieved)} units from {pid} to poset')
        for unit in units_recieved:
            parents = [poset.unit_by_hash(parent_hash) for parent_hash in unit['parents_hashes']]
            U = Unit(unit['creator_id'], parents, unit['txs'], unit['signature'], unit['coinshares'])
            if poset.check_compliance(U):
                poset.add_unit(U)
            else:
                logger.info(f'listener: got unit from {pid} that does not comply to the rules; aborting')
                n_recv_syncs -= 1
                return
        logger.info(f'listener: units from {pid} are added succesful')

        send_ind = [i for i, (int_height, ex_height) in enumerate(zip(int_heights, ex_heights)) if int_height > ex_height]

        # send units
        logger.info(f'listener: sending units to {pid}')
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
        logger.info(f'listener: units sent to {pid}')

        logger.info(f'listener: syncing with {pid} completed succesfully')
        n_recv_syncs -= 1



    server = await asyncio.start_server(listen_handler, host_addr[0], host_addr[1])

    logger = logging.getLogger(LOGGING_FILENAME)
    logger.info(f'Serving on {host_addr}')

    async with server:
        await server.serve_forever()



async def sync(poset, pid, peer_addr):
    logger = logging.getLogger(LOGGING_FILENAME)

    logger.info(f'sync: establishing connection to {pid}')
    reader, writer = await asyncio.open_connection(peer_addr[0], peer_addr[1])
    logger.info(f'sync: established connection to {pid}')

    int_heights, int_hashes = poset.get_max_heights_hashes()

    logger.info(f'sync: sending info about forkers and heights&hashes to {pid}')
    data = marshal.dumps((int_heights, int_heights))
    writer.write(str(len(data)).encode())
    writer.write(b'\n')
    writer.write(data)
    await writer.drain()
    logger.info(f'sync: sent forkers/heights {int_heights} to {pid}')


    logger.info(f'sync: receiving info about forkers and heights&hashes from {pid}')
    data = await reader.readuntil()
    n_bytes = int(data[:-1])
    data = await reader.read(n_bytes)
    ex_heights, ex_hashes = marshal.loads(data)
    logger.info(f'sync: got forkers/heights {ex_heights} from {pid}')

    # send units
    send_ind = [i for i, (int_height, ex_height) in enumerate(zip(int_heights, ex_heights)) if int_height > ex_height]
    logger.info(f'sync: sending units to {pid}')
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
    logger.info(f'sync: units sent to {pid}')

    # receive units
    logger.info(f'sync: receiving units from {pid}')
    data = await reader.readuntil()
    n_bytes = int(data[:-1])
    data = await reader.read(n_bytes)
    units_recieved = marshal.loads(data)
    logger.info('sync: received units')

    logger.info(f'sync: trying to add {len(units_recieved)} units from {pid} to poset')
    for unit in units_recieved:
        parents = [poset.unit_by_hash(parent_hash) for parent_hash in unit['parents_hashes']]
        U = Unit(unit['creator_id'], parents, unit['txs'], unit['signature'], unit['coinshares'])
        if poset.check_compliance(U):
            poset.add_unit(U)
        else:
            logger.info(f'sync: got unit from {pid} that does not comply to the rules; aborting')
            return
    logger.info(f'sync: units from {pid} are added succesful')


    logger.info(f'sync: syncing with {pid} completed succesfully')



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
