from aleph.data_structures import Unit, Poset
from aleph.utils import dag_utils
from aleph.utils.dag import DAG
import random



def check_create_unit(n_processes, n_units, n_forkers, strategy, verify_fails = False):
    forkers = random.sample(range(n_processes), n_forkers)
    non_forkers = [process_id for process_id in range(n_processes) if process_id not in forkers]
    dag = DAG(n_processes)
    posets = [Poset(n_processes = n_processes) for process_id in range(n_processes)]
    name_to_unit = [{} for process_id in range(n_processes)]
    unit_to_name = [{} for process_id in range(n_processes)]

    iter_count = 0
    while len(dag) < n_units:
        iter_count += 1
        assert iter_count < n_units*5000, "Creating %d units seems to be taking too long." % n_units
        creator_id = random.choice(range(n_processes))
        if creator_id in forkers:
            res = dag_utils.generate_random_compliant_unit(dag, n_processes, creator_id, forking = True)
            if res is None:
                continue
            name, parents = res
        else:
            U = posets[creator_id].create_unit(creator_id, [], strategy = strategy, num_parents = 2)
            if U is None:
                if verify_fails:
                    res = dag_utils.generate_random_compliant_unit(dag, n_processes, creator_id, forking = False)
                    assert res is None
                continue
            parents = [unit_to_name[creator_id][V] for V in U.parents]
            name = dag_utils.generate_unused_name(dag, creator_id)
            #print(name)

        dag.add(name, creator_id, parents)

        for process_id in non_forkers:
            parent_units = [name_to_unit[process_id][parent_name] for parent_name in parents]
            U = Unit(creator_id = creator_id, parents = parent_units, txs = [])
            assert posets[process_id].check_compliance(U)
            posets[process_id].add_unit(U)
            name_to_unit[process_id][name] = U
            unit_to_name[process_id][U] = name



def test_create_unit_small():
    random.seed(123456789)
    repetitions = 50
    for strategy in ["link_self_predecessor", "link_above_self_predecessor"]:
        for rep in range(repetitions):
            n_processes = random.randint(4, 15)
            n_units = random.randint(0, n_processes*5)
            n_forkers = random.randint(0,n_processes//3)
            check_create_unit(n_processes, n_units, n_forkers, strategy, verify_fails = True)



def test_create_unit_large():
    random.seed(123456789)
    repetitions = 5
    for strategy in ["link_self_predecessor", "link_above_self_predecessor"]:
        for rep in range(repetitions):
            n_processes = random.randint(30, 80)
            n_units = random.randint(0, n_processes*3)
            n_forkers = random.randint(0,n_processes//3)
            check_create_unit(n_processes, n_units, n_forkers, strategy, verify_fails = True)


