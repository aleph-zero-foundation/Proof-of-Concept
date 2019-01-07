import random
from itertools import cycle

from aleph.data_structures import Unit, Poset, UserDB
from aleph.process import Process
from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.utils.dag_utils import generate_random_compliant_unit
from aleph.utils import DAG, dag_utils
from aleph.utils.testing_utils import add_to_instance
from aleph.utils.plot import plot_poset, plot_dag



def add_new_unit_with_given_parents(process, processes, dag, names_to_units, maximal_names, creator_id, parent_ids_set):
    tries = 0
    success = False
    n_processes = process.n_processes

    if parent_ids_set == []:
        parent_names = []
        success = True
    else:
        for second_parent in parent_ids_set:
            if second_parent == creator_id:
                continue
            parent_ids = [creator_id, second_parent]
            parent_names = [maximal_names[parent_id] for parent_id in parent_ids]
            if dag_utils.check_parent_diversity(dag, creator_id, parent_names, (n_processes+2)//3):
                success = True
                break

    assert success, "Adding a new unit failed because it was impossible to choose diverse parents"

    new_name = dag_utils.generate_unused_name(dag, creator_id)
    dag.add(new_name, creator_id, parent_names)
    maximal_names[creator_id] = new_name
    parent_units = [names_to_units[nm] for nm in parent_names]
    U = Unit(creator_id, parent_units, txs=[])
    processes[creator_id].sign_unit(U)
    names_to_units[new_name] = U
    process.poset.prepare_unit(U)
    assert process.add_unit_to_poset(U), f'Unit {new_name} not compliant.'



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

    maximal_names = [None for _ in range(n_processes)]

    byzantine = [0]
    correct = list(range(1,n_processes))

    # create dealing units for every process
    for process_id in range(n_processes):
        add_new_unit_with_given_parents(process, processes, dag, names_to_units, maximal_names, process_id, [])


    # reach level 1 with processes 1,2,3 while "ignoring" process 0
    while True:
        for process_id in correct:
            #print(process_id)
            name = maximal_names[process_id]
            print(names_to_units[name].level)
            if names_to_units[name].level<1:
                # choose parents in a random order

                parent_ids_set = correct[:]
                random.shuffle(parent_ids_set)
                add_new_unit_with_given_parents(process, processes, dag, names_to_units, maximal_names, process_id, parent_ids_set)

        if not any(names_to_units[name].level<1 for name in [maximal_names[proc_id] for proc_id in correct]):
            break

    # add a new unit for process 0 so as to reach level 1
    add_new_unit_with_given_parents(process, processes, dag, names_to_units, maximal_names, 0, correct)

    # reach level 2 with processes 1,2,3 while "ignoring" process 0
    while True:
        for process_id in correct:
            name = maximal_names[process_id]
            if names_to_units[name].level<2:
                # choose parents in a random order

                parent_ids_set = correct
                random.shuffle(parent_ids_set)
                add_new_unit_with_given_parents(process, processes, dag, names_to_units, maximal_names, process_id, parent_ids_set)

        if not any(names_to_units[name].level<2 for name in [maximal_names[proc_id] for proc_id in correct]):
            break

    add_new_unit_with_given_parents(process, processes, dag, names_to_units, maximal_names, 0, correct)

    plot_dag(dag)

    '''



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

    '''



test_delta_level_4()