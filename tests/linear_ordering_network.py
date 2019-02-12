import asyncio
import multiprocessing
import socket
import random

from aleph.network import tx_generator, tx_source_gen
from aleph.data_structures import Poset, UserDB, Tx
from aleph.process import Process
from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.utils.dag_utils import generate_random_forking, poset_from_dag

import aleph.const as consts


async def main():
    n_processes = 16
    n_forkers = 0
    use_tcoin = False
    consts.UNITS_LIMIT = 50

    processes = []
    host_ports = [8900+i for i in range(n_processes)]
    local_ip = socket.gethostbyname(socket.gethostname())
    addresses = [(local_ip, port) for port in host_ports]
    recv_addresses = [(local_ip, 9100+i) for i in range(n_processes)]

    signing_keys = [SigningKey() for _ in range(n_processes)]
    public_keys = [VerifyKey.from_SigningKey(sk) for sk in signing_keys]

    tasks = []
    userDB = None

    for process_id in range(n_processes):
        sk = signing_keys[process_id]
        pk = public_keys[process_id]
        new_process = Process(n_processes,
                      process_id,
                      sk, pk,
                      addresses,
                      public_keys,
                      None,
                      userDB,
                      'LINEAR_ORDERING',
                      tx_source_gen(batch_size=3, txpu=1, seed=process_id))
        processes.append(new_process)
        tasks.append(asyncio.create_task(new_process.run()))

    await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())
