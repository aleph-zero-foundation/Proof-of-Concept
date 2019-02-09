import asyncio
import multiprocessing
import socket
import random

from aleph.network import tx_generator
from aleph.data_structures import Poset, UserDB, Tx
from aleph.process import Process
from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.utils.dag_utils import generate_random_forking, poset_from_dag

import aleph.const as consts


def tx_source_gen(process_id, n_processes, batch_size, txpu):
    '''
    Produces a tx generator and ensures that all processes choose batches from the same set.
    :param int batch_size: number of txs for a process to input into the system.
    :param int n_processes: number of parties.
    :param int txpu: number of txs to be included in one unit.
    '''

    def _tx_source(tx_receiver_address, tx_queue):
        '''
        Generates transactions in bundles of size txpu till batch_size is reached
        :param None tx_receiver_address: needed only for comatibility of args list with network.tx_listener
        :param queue tx_queue: queue for newly generated txs
        '''
        # ensure that batches are different
        random.seed(process_id)
        with open('light_nodes_public_keys', 'r') as f:
            ln_public_keys = [line[:-1] for line in f]

        proposed = 0
        while proposed<batch_size:
            if proposed+txpu <= batch_size:
                offset = txpu
            else:
                offset = batch_size - proposed

            txs = []
            for _ in range(offset):
                source = random.choice(ln_public_keys)
                target = random.choice(ln_public_keys)
                amount = random.randint(1, 32767)
                txs.append(Tx(source, target, amount))

            proposed += offset

            tx_queue.put(txs, block=True)

    return _tx_source


async def main():
    n_processes = 32
    n_forkers = 0
    txps = 50
    n_light_nodes = 100
    use_tcoin = False
    consts.UNITS_LIMIT = 200

    processes = []
    host_ports = [8900+i for i in range(n_processes)]
    local_ip = socket.gethostbyname(socket.gethostname())
    addresses = [(local_ip, port) for port in host_ports]
    recv_addresses = [(local_ip, 9100+i) for i in range(n_processes)]

    signing_keys = [SigningKey() for _ in range(n_processes)]
    public_keys = [VerifyKey.from_SigningKey(sk) for sk in signing_keys]

    tasks = []

    initial_balances_and_indices = []
    ln_signing_keys = [SigningKey() for _ in range(n_light_nodes)]
    ln_public_keys = [VerifyKey.from_SigningKey(sk) for sk in ln_signing_keys]
    for i in range(n_light_nodes):
        initial_balances_and_indices.append((ln_public_keys[i].to_hex(), random.randrange(10000, 100000), -1))
    userDB = None

    stop_conditions = dict(zip(['n_create', 'n_sync', 'n_level'], [-1, -1, 10]))

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
                      tx_source_gen(process_id, n_processes, 3,1))
        processes.append(new_process)
        tasks.append(asyncio.create_task(new_process.run()))

    await asyncio.sleep(1)


    await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())
    # mp_main()



