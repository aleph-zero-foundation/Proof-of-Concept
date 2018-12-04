from aleph.network import listener, sync
from aleph.data_structures import Poset
from aleph.crypto.signatures.keys import PrivateKey, PublicKey
from aleph.utils.dag_utils import generate_random_forking, poset_from_dag
from aleph.utils.plot import plot_poset, plot_dag

import asyncio


async def main():
    n_processes = 5
    n_units = 30
    n_forkers = 0

    dag = generate_random_forking(n_processes, n_units, n_forkers)

    n_parties = 2
    posets = []
    host_ports = [8888+i for i in range(n_processes)]
    addresses = [('127.0.0.1', port) for port in host_ports]

    tasks = []

    for process_id in range(n_parties):
        sk = PrivateKey()
        pk = PublicKey(sk)
        poset = poset_from_dag(dag)[0]
        poset.id = process_id
        posets.append(poset)

        # await listener(poset, host_ip, host_port)
        tasks.append(asyncio.create_task(listener(poset, process_id, addresses)))

    U = posets[0].create_unit(0, txs=[], strategy="link_self_predecessor", num_parents=2)
    posets[0].add_unit(U)
    U = posets[1].create_unit(1, txs=[], strategy="link_self_predecessor", num_parents=2)
    posets[1].add_unit(U)
    # wait for servers to start
    await asyncio.sleep(1)
    # sync!
    tasks.append(asyncio.create_task(sync(posets[1], 1, 0, addresses[0])))

    await tasks[-1]
    tasks[0].cancel()
    tasks[1].cancel()
    # await asyncio.gather(*asyncio.all_tasks())

asyncio.run(main())
