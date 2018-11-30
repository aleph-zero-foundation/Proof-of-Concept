from aleph.network import listener, connecter
from aleph.data_structures import Poset
from aleph.crypto.signatures.keys import PrivateKey, PublicKey
from aleph.utils.dag_utils import generate_random_forking, poset_from_dag
from aleph.utils.plot import plot_poset, plot_dag

import asyncio

import asyncio

class Queue():
    def __init__(self):
        self.items = []

    def get(self):
        if self.items:
            return self.items.pop(0)
        else:
            return None

    def put(self, item):
        self.items.append(item)

    def __bool__(self):
        return self.items != []

async def add_units_from_queue(poset, queue):
    #print(poset.id, 'adding unit coro start')
    while True:
        if queue:
            U = queue.get()
            print('adding unit', U, 'to poset', poset.id)
            poset.add_unit(U)
            break
        else:
            #print(poset.id, 'adding units, nothing to add, sleeping')
            await asyncio.sleep(1)

async def sync(posets, queues, host_ip, host_ports, sender, recipient):
    await connecter(posets[sender], queues[sender], host_ip, host_ports[recipient])


async def main():
    n_processes = 5
    n_units = 30
    n_forkers = 0

    dag = generate_random_forking(n_processes, n_units, n_forkers)

    n_parties = 2
    posets = []
    host_ip = '127.0.0.1'
    host_ports = []
    queues = []

    tasks = []

    for process_id in range(n_parties):
        sk = PrivateKey()
        pk = PublicKey(sk)
        poset = poset_from_dag(dag, sk, pk)[0]
        poset.id = process_id
        posets.append(poset)
        host_port = 8888 + process_id
        host_ports.append(host_port)
        #queue = asyncio.Queue()
        queue = Queue()
        queues.append(queue)

        # await listener(poset, queue, host_ip, host_port)
        tasks.append(asyncio.create_task(listener(poset, queue, host_ip, host_port)))

    for process_id in range(n_parties):
        tasks.append(asyncio.create_task(add_units_from_queue(posets[process_id], queues[process_id])))

    U = posets[0].create_unit(txs=[], strategy="link_self_predecessor", num_parents=2)
    posets[0].add_unit(U)
    U = posets[1].create_unit(txs=[], strategy="link_self_predecessor", num_parents=2)
    posets[1].add_unit(U)
    tasks.append(asyncio.create_task(sync(posets, queues, host_ip, host_ports, 1,0)))


    await asyncio.gather(*asyncio.all_tasks())

asyncio.run(main())
