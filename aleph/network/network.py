import asyncio
import marshal

from aleph.config import HOST_IP, HOST_PORT
from aleph.data_structures import Unit


def unit_to_dict(U):
    parents_hashes = [parent.hash() for parent in U.parents]
    return {'creator_id': U.creator_id,
            'parents_hashes': parents_hashes,
            'txs': U.txs,
            'signature': U.signature,
            'coinshares': U.coinshares}

async def listener(poset, queue, host_ip, host_port):
    async def listen_handler(reader, writer):
        print('listener: connection established')
        int_heights, int_hashes = poset.get_max_heights_hashes()

        print('listener: reading hh')
        data = await reader.readuntil()
        n_bytes = int(data[:-1])
        print('listener: hh n_bytes', n_bytes)
        data = await reader.read(n_bytes)
        ex_heights, ex_hashes = marshal.loads(data)
        print('listener: got hh')

        print('listener: writing hh')
        data = marshal.dumps((int_heights, int_heights))
        print('listener: writing hh bytes', len(data))
        writer.write(str(len(data)).encode())
        writer.write(b'\n')
        writer.write(data)
        await writer.drain()
        #writer.write_eof()
        print('listener: wrote hh')

        # receive units
        print('listener: receiving units')
        data = await reader.readuntil()
        n_bytes = int(data[:-1])
        print('listener: units n_bytes', n_bytes)
        data = await reader.read(n_bytes)
        units_recieved = marshal.loads(data)
        print('listener: received units', len(units_recieved))


        print('listener: adding units to queue', len(units_recieved))
        for unit in units_recieved:
            parents = [poset.unit_by_hash(parent_hash) for parent_hash in unit['parents_hashes']]
            U = Unit(unit['creator_id'], parents, unit['txs'], unit['signature'], unit['coinshares'])
            queue.put(U)
        print('listener: units added')

        send_ind = [i for i, (int_height, ex_height) in enumerate(zip(int_heights, ex_heights)) if int_height > ex_height]
        print('listener: send_ind = ', send_ind)

        # send units
        print('listener: sending units')
        units_to_send = []
        for i in send_ind:
            units = poset.units_by_height(creator_id=i, min_height=ex_heights[i]+1, max_height=int_heights[i])
            units = [unit_to_dict(U) for U in units]
            units_to_send.extend(units)
        data = marshal.dumps(units_to_send)
        print('linsener: writing unites bytes', len(data))
        writer.write(str(len(data)).encode())
        writer.write(b'\n')
        writer.write(data)
        await writer.drain()
        #writer.write_eof()
        print('listener: units send')

        print('listener: job complete')

    server = await asyncio.start_server(listen_handler, host_ip, host_port)

    addr = server.sockets[0].getsockname()
    print(f'Serving on {addr}')

    async with server:
        await server.serve_forever()


async def connecter(poset, queue, recipient_ip, recipient_port):
    await asyncio.sleep(1)
    int_heights, int_hashes = poset.get_max_heights_hashes()

    reader, writer = await asyncio.open_connection(recipient_ip, recipient_port)
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

    print('connecter: adding units to queue', len(units_recieved))
    for unit in units_recieved:
        parents = [poset.unit_by_hash(parent_hash) for parent_hash in unit['parents_hashes']]
        U = Unit(unit['creator_id'], parents, unit['txs'], unit['signature'], unit['coinshares'])
        queue.put(U)
    print('connecter: units added')

    print('connecter: job complete')



class Network:

    def __init__(self, addresses):
        pass


    def listen(self):
        pass


    def knock(self, process_id):
        pass


class Channel:

    def __init__(self, ):
        pass

    def receive(self):
        pass

    def send(self, unit):
        pass
