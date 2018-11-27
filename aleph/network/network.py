import asyncio
import marshal

from aleph.config import HOST_IP, HOST_PORT
from aleph.data_structures import Unit


def unit_to_dict(U):
    parents_hashes = [parent.hash() for parent in U.parents]
    return {'creator_id': U.creator_id,
            'parents_hashes': parents_hashes,
            'txs': U.txs,
            'signature', U.signature,
            'coinshares', U.coinshares}

async def listener(poset, queue, host_ip, host_port):
    async def listen_handler(reader, writer):
        int_heights, int_hashes = poset.get_max_heights_hashes()

        data = await reader.read()
        ex_heights, ex_hashes = marshal.loads(data.decode())

        data = marshal.dumps((int_heights, int_heights))
        await writer.write(data)

        # receive units
        data = await reader.read()
        units_recieved = marshal.loads(data.decode())

        for unit in units_recieved:
            parents = [poset.unit_by_hash(parent_hash) for parent_hash in unit['parents_hashes']]
            U = Unit(unit['creator_id'], parents, unit['txs'], unit['signature'], unit['coinshares'])
            queue.put(U)

        send_ind = [i for i, int_height, ex_height for enumerate(zip(int_heights, ex_heights)) if int_height > ex_height]

        # send units
        units_to_send = []
        for i in send_ind:
            units = poset.get_units_by_height(creator_id=i, min_height=ex_heights[i]+1, max_height=int_heights[i])
            units = [unit_to_dict(U) for U in units]
            units_to_send.extend(units)
        data = marshal.dumps(units_to_send)
        await writer.write(data)



    server = await asyncio.start_server(listen_handler, host_ip, host_port)

    addr = server.sockets[0].getsockname()
    print(f'Serving on {addr}')

    async with server:
        await server.serve_forever()

async def connecter():
    pass



class Network:

    def __init__(self, addresses):
        pass


    def listen(self):
        pass


    def knock(self, process_id):


class Channel:

    def __init__(self, ):
        pass

    def receive(self):
        pass

    def send(self, unit):
        pass       pass
