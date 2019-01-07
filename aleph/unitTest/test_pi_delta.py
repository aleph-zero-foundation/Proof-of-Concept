import random
from itertools import cycle

from aleph.data_structures import Unit, Poset, UserDB
from aleph.process import Process
from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.utils.dag_utils import generate_random_compliant_unit
from aleph.utils import DAG, dag_utils
from aleph.utils.testing_utils import add_to_instance


def test_delta_level_4():

    n_processes = 4


    processes = []
    host_ports = [8900+i for i in range(n_processes)]
    addresses = [('127.0.0.1', port) for port in host_ports]
    recv_addresses = [('127.0.0.1', 9100+i) for i in range(n_processes)]

    signing_keys = [SigningKey() for _ in range(n_processes)]
    public_keys = [VerifyKey.from_SigningKey(sk) for sk in signing_keys]

    for process_id in range(n_processes):
        sk = signing_keys[process_id]
        pk = public_keys[process_id]
        new_process = Process(n_processes, process_id, sk, pk, addresses, public_keys, recv_addresses[process_id], None, 'LINEAR_ORDERING')
        processes.append(new_process)

    process = processes[0]

    dag = DAG(n_processes)
    names_to_units = {}

    for process_id in range(n_processes):
        name = dag_utils.generate_unit_name(0, process_id)
        dag.add(name, process_id, [])
        U = Unit(process_id, [], txs=[])
        processes[process_id].sign_unit(U)
        names_to_units[name] = U
        process.poset.prepare_unit(U)
        assert process.add_unit_to_poset(U), f'Unit {name} not compliant.'

    byzantine = [0]
    correct = list(range(1,n_processes))

    while True:
        maximal_per_process = [dag.maximal_units_per_process(process_id)[0] for process_id in range(n_processes)]
        for process_id in correct:
            name = dag.maximal_units_per_process(process_id)[0]
            if names_to_units[name].level<1:
                link_to = correct[correct.index() + 1)%len(correct)]

                parents =
        if not any(names_to_units[name].level<1 for name in maximal_per_process):
            break




    for unit_no in range(n_units):
        while True:
            creator_id = random.choice(range(n_processes))
            gen_unit = generate_random_compliant_unit(dag, n_processes, process_id = creator_id, forking = False, only_maximal_parents = True)
            if gen_unit is not None:
                name, parent_names = gen_unit
                break
        #print(name, parent_names)
        #print(creator_id)
        parents = [names_to_units[par_name] for par_name in parent_names]
        U = Unit(creator_id, parents, txs=[])
        processes[creator_id].sign_unit(U)
        names_to_units[name] = U
        process.poset.prepare_unit(U)
        if not process.add_unit_to_poset(U):
            print(f'Unit {name} not compliant.')
            exit(0)
        dag.add(name, creator_id, parent_names)
        if unit_no%100 == 0:
            print(f"Adding unit no {unit_no + n_processes} out of {n_units + n_processes}.")



