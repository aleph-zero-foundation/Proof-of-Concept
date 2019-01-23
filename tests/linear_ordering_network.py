import asyncio
import multiprocessing
import random

from aleph.network import tx_generator
from aleph.data_structures import Poset, UserDB
from aleph.process import Process
from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.utils.dag_utils import generate_random_forking, poset_from_dag


async def main():
    n_processes = 16
    n_forkers = 0
    txps = 50
    n_light_nodes = 100
    use_tcoin = False

    processes = []
    host_ports = [8900+i for i in range(n_processes)]
    addresses = [('127.0.0.1', port) for port in host_ports]
    recv_addresses = [('127.0.0.1', 9100+i) for i in range(n_processes)]

    signing_keys = [SigningKey() for _ in range(n_processes)]
    public_keys = [VerifyKey.from_SigningKey(sk) for sk in signing_keys]

    tasks = []

    initial_balances_and_indices = []
    ln_signing_keys = [SigningKey() for _ in range(n_light_nodes)]
    ln_public_keys = [VerifyKey.from_SigningKey(sk) for sk in ln_signing_keys]
    for i in range(n_light_nodes):
        initial_balances_and_indices.append((ln_public_keys[i].to_hex(), random.randrange(10000, 100000), -1))
    userDB = UserDB(initial_balances_and_indices)


    for process_id in range(n_processes):
        sk = signing_keys[process_id]
        pk = public_keys[process_id]
        if use_tcoin:
            new_process = Process(n_processes, process_id, sk, pk, addresses, public_keys, recv_addresses[process_id], userDB, 'LINEAR_ORDERING', True)
        else:
            new_process = Process(n_processes, process_id, sk, pk, addresses, public_keys, recv_addresses[process_id], userDB, 'LINEAR_ORDERING')
        processes.append(new_process)
        tasks.append(asyncio.create_task(new_process.run()))

    await asyncio.sleep(1)

    p = multiprocessing.Process(target=tx_generator, args=(recv_addresses, ln_signing_keys, txps))
    p.start()

    await asyncio.gather(*tasks)
    p.kill()


if __name__ == '__main__':
    asyncio.run(main())
    # mp_main()



