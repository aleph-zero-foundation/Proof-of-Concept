from aleph.network import listener, sync, tx_generator
from aleph.data_structures import Poset
from aleph.process import Process
from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.utils.dag_utils import generate_random_forking, poset_from_dag
from aleph.utils.plot import plot_poset, plot_dag

import asyncio
import multiprocessing


async def main():
    n_processes = 5
    n_units = 0
    n_forkers = 0
    txps = 30
    n_light_nodes = 100

    processes = []
    host_ports = [8900+i for i in range(n_processes)]
    addresses = [('127.0.0.1', port) for port in host_ports]
    recv_addresses = [('127.0.0.1', 9100+i) for i in range(n_processes)]

    signing_keys = [SigningKey() for _ in range(n_processes)]
    public_keys = [VerifyKey.from_SigningKey(sk) for sk in signing_keys]

    tasks = []

    for process_id in range(n_processes):
        sk = signing_keys[process_id]
        pk = public_keys[process_id]
        new_process = Process(n_processes, process_id, sk, pk, addresses, public_keys, recv_addresses[process_id])
        processes.append(new_process)
        tasks.append(asyncio.create_task(new_process.run()))

    await asyncio.sleep(1)
    p = multiprocessing.Process(target=tx_generator, args=(recv_addresses, n_light_nodes, txps))
    p.start()

    await asyncio.gather(*tasks)
    p.kill()


if __name__ == '__main__':
    asyncio.run(main())
    # mp_main()



def f(n_processes, process_id, sk, pk, addresses, public_keys):
        new_process = Process(n_processes, process_id, sk, pk, addresses, public_keys, None)
        new_process.poset = Poset(n_processes)
        asyncio.run(new_process.run())

def mp_main():
    n_processes = 1
    n_units = 0
    n_forkers = 0

    host_ports = [8888+i for i in range(n_processes)]
    addresses = [('127.0.0.1', port) for port in host_ports]

    signing_keys = [SigningKey() for _ in range(n_processes)]
    public_keys = [VerifyKey.from_SigningKey(sk) for sk in signing_keys]

    procs = [multiprocessing.Process(target=f, args=(n_processes, process_id, sk, pk, addresses, public_keys)) for process_id, sk, pk in zip(range(n_processes), signing_keys, public_keys)]
    for p in procs:
        p.start()
