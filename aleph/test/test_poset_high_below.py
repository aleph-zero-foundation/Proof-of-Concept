from aleph.utils import dag_utils
from aleph.utils.generic_test import generate_and_check_dag
import random



def test_small_nonforking_below():
    generate_and_check_dag(
        checks= [check_all_pairs_high_below],
        n_processes = 5,
        n_units = 50,
        repetitions = 1,
    )

def test_large_nonforking_below():
    generate_and_check_dag(
        checks= [check_all_pairs_high_below],
        n_processes = 30,
        n_units = 100,
        repetitions = 1,
    )


def test_small_forking_below():
    generate_and_check_dag(
        checks= [check_all_pairs_high_below],
        n_processes = 5,
        n_units = 50,
        repetitions = 30,
        forking = lambda: 1
    )


def test_large_forking_below():
    generate_and_check_dag(
        checks= [check_all_pairs_high_below],
        n_processes = 30,
        n_units = 100,
        repetitions = 10,
        forking = lambda: 2
    )


def check_all_pairs_high_below(arg):
    '''
    Create a poset from a dag and test U<<V for all pairs of units U,V
    against a naive BFS-implementation from dag_utils
    '''
    poset, unit_dict = dag_utils.poset_from_dag(arg)

    for nodeU, U in unit_dict.items():
        for nodeV, V in unit_dict.items():
            threshold = (2 * arg.n_processes + 2) // 3
            assert poset.high_below(U,V) == arg.is_reachable_through_n_intermediate(nodeU, nodeV, threshold)
            assert poset.high_above(U,V) == arg.is_reachable_through_n_intermediate(nodeV, nodeU, threshold)
