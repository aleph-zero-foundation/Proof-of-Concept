from aleph.network import listener, connecter, sync
from aleph.data_structures import Poset
from aleph.process import Process
from aleph.crypto.signatures.keys import PrivateKey, PublicKey
from aleph.utils.dag_utils import generate_random_forking, poset_from_dag
from aleph.utils.plot import plot_poset, plot_dag

import asyncio


async def test_processes():
    n_processes = 4
    n_units = 0
    n_forkers = 0

    #dag = generate_random_forking(n_processes, n_units, n_forkers)

    processes = []
    host_ports = [8888+i for i in range(n_processes)]
    addresses = [('127.0.0.1', port) for port in host_ports]

    private_keys = [PrivateKey() for _ in range(n_processes)]
    public_keys = [PublicKey(sk) for sk in private_keys]

    tasks = []

    for process_id in range(n_processes):
        sk = private_keys[process_id]
        pk = public_keys[process_id]
        new_process = Process(n_processes, process_id, sk, pk, addresses, public_keys)
        new_process.poset = Poset(n_processes)
        processes.append(new_process)
        tasks.append(asyncio.create_task(new_process.run()))

    await asyncio.gather(*tasks)


asyncio.run(test_processes())
