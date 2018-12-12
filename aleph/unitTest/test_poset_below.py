from aleph.data_structures import Unit, Poset
from aleph.utils import dag_utils
import random


def test_trivial_single_level_below():
    n_processes = 4
    poset = Poset(n_processes = n_processes)

    bottom_units_per_process = [Unit(creator_id = i, parents = [], txs = []) for i in range(n_processes)]
    for i in range(n_processes):
        poset.add_unit(bottom_units_per_process[i])

    for i in range(n_processes):
        assert poset.max_units_per_process[i][0] is bottom_units_per_process[i]

    U0 = poset.max_units_per_process[0][0]
    U1 = poset.max_units_per_process[1][0]
    U2 = poset.max_units_per_process[2][0]
    U3 = poset.max_units_per_process[3][0]

    U = Unit(creator_id = 0, parents = [U0, U1], txs = [])

    poset.add_unit(U)
    assert poset.below(U0, U)
    assert poset.above(U, U0)
    assert poset.below(U1, U)
    assert not poset.below(U2, U)
    assert not poset.below(U3, U)


def test_small_nonforking_below():
    random.seed(123456789)
    n_processes = 5
    n_units = 50
    repetitions = 30
    for _ in range(repetitions):
        dag = dag_utils.generate_random_nonforking(n_processes, n_units)
        check_all_pairs_below(dag)


def test_large_nonforking_below():
    random.seed(123456789)
    n_processes = 100
    n_units = 200
    repetitions = 1
    for _ in range(repetitions):
        dag = dag_utils.generate_random_nonforking(n_processes, n_units)
        check_all_pairs_below(dag)


def test_small_forking_below():
    random.seed(123456789)
    n_processes = 5
    n_units = 50
    repetitions = 30
    for _ in range(repetitions):
        n_forking = random.randint(0,n_processes)
        dag = dag_utils.generate_random_forking(n_processes, n_units, n_forking)
        check_all_pairs_below(dag)


def test_large_forking_below():
    random.seed(123456789)
    n_processes = 100
    n_units = 200
    repetitions = 1
    for _ in range(repetitions):
        n_forking = random.randint(0,n_processes)
        dag = dag_utils.generate_random_forking(n_processes, n_units, n_forking)
        check_all_pairs_below(dag)


def check_all_pairs_below(arg):
    '''
    Create a poset from a dag and test U<=V for all pairs of units U,V
    against a naive BFS-implementation from dag_utils
    '''
    poset, unit_dict = dag_utils.poset_from_dag(arg)

    for nodeU, U in unit_dict.items():
        for nodeV, V in unit_dict.items():
            assert poset.below(U,V) == arg.is_reachable(nodeU, nodeV)
            assert poset.above(U,V) == arg.is_reachable(nodeV, nodeU)
