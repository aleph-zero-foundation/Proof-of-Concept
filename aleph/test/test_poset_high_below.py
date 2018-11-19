from aleph.data_structures import Unit, Poset
from aleph.utils import dag_utils
import random



def test_small_nonforking_below():
    random.seed(123456789)
    n_processes = 5
    n_units = 50
    repetitions = 1
    for _ in range(repetitions):
        dag = dag_utils.generate_random_nonforking(n_processes, n_units)
        check_all_pairs_high_below(dag, n_processes)


def test_large_nonforking_below():
    random.seed(123456789)
    n_processes = 30
    n_units = 100
    repetitions = 1
    for _ in range(repetitions):
        dag = dag_utils.generate_random_nonforking(n_processes, n_units)
        check_all_pairs_high_below(dag, n_processes)


def test_small_forking_below():
    random.seed(123456789)
    n_processes = 5
    n_units = 50
    repetitions = 30
    for _ in range(repetitions):
        #n_forking = random.randint(0,n_processes)
        n_forking = 1
        dag = dag_utils.generate_random_forking(n_processes, n_units, n_forking)
        dag_utils.dag_to_file(dag, n_processes, 'bad_test.txt')
        check_all_pairs_high_below(dag, n_processes)

def test_large_forking_below():
    random.seed(123456789)
    n_processes = 30
    n_units = 100
    repetitions = 10
    for _ in range(repetitions):
        #n_forking = random.randint(0,n_processes)
        n_forking = 2
        dag = dag_utils.generate_random_forking(n_processes, n_units, n_forking)
        check_all_pairs_high_below(dag, n_processes)


def check_all_pairs_high_below(dag, n_processes):
    '''
    Create a poset from a dag and test U<<V for all pairs of units U,V
    against a naive BFS-implementation from dag_utils
    '''
    poset, unit_dict = dag_utils.poset_from_dag(dag, n_processes)

    for nameU, U in unit_dict.items():
        for nameV, V in unit_dict.items():
            nodeU = (nameU, U.creator_id)
            nodeV = (nameV, V.creator_id)
            # ceil((2/3)*n_processes)
            treshold = (2*n_processes + 2)//3
            assert poset.high_below(U,V) == dag_utils.is_reachable_through_n_intermediate(dag, nodeU, nodeV, treshold)
            assert poset.high_above(U,V) == dag_utils.is_reachable_through_n_intermediate(dag, nodeV, nodeU, treshold)

