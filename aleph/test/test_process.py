from aleph.network import listener, connecter, sync
from aleph.data_structures import Poset
from aleph.process import Process
from aleph.crypto.signatures.keys import PrivateKey, PublicKey
from aleph.utils.dag_utils import generate_random_forking, poset_from_dag
from aleph.utils.plot import plot_poset, plot_dag

import asyncio


def test_processes():
    n_processes = 4
    n_units = 30
    n_forkers = 0

    dag = generate_random_forking(n_processes, n_units, n_forkers)

    n_parties = 4
    posets = []
    processes = []
    host_ports = [8888+i for i in range(n_parties)]
    addresses = [('127.0.0.1', port) for port in host_ports]

    private_keys = [PrivateKey() for _ in range(n_processes)]
    public_keys = [PublicKey(sk) for sk in private_keys]

    for process_id in range(n_parties):
        sk = private_keys[process_id]
        pk = public_keys[process_id]

        new_process = Process(n_processes, process_id, sk, pk, addresses, public_keys)
        new_process.poset = poset_from_dag(dag)[0]
        processes.append(new_process)
        new_process.run()


test_processes()
