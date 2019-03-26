from aleph.data_structures import Unit, Poset
from aleph.utils import dag_utils
from aleph.utils.generic_test import generate_and_check_dag
import random


def test_trivial_single_level_below():
    '''
    A simple manual test that makes sure that poset.max_units_per_process are correctly set when only dealing units are present
        and that after adding one additional unit to the poset, Poset.below returns correct results.
    '''

    # start by creating a poset with 4 processes and add the 4 dealing units to it
    n_processes = 4
    poset = Poset(n_processes = n_processes, use_tcoin = False)
    dealing_units_per_process = [Unit(creator_id = i, parents = [], txs = []) for i in range(n_processes)]
    for i in range(n_processes):
        poset.prepare_unit(dealing_units_per_process[i])
        poset.add_unit(dealing_units_per_process[i])

    # make sure that dealing units are set as maximal units in the poset
    for i in range(n_processes):
        assert poset.max_units_per_process[i][0] is dealing_units_per_process[i]

    U0 = poset.max_units_per_process[0][0]
    U1 = poset.max_units_per_process[1][0]
    U2 = poset.max_units_per_process[2][0]
    U3 = poset.max_units_per_process[3][0]

    # add one new unit with parents U0, U1 to the poset
    U = Unit(creator_id = 0, parents = [U0, U1], txs = [])
    poset.prepare_unit(U)
    poset.add_unit(U)

    assert poset.below(U0, U)
    assert poset.above(U, U0)
    assert poset.below(U1, U)
    assert not poset.below(U2, U)
    assert not poset.below(U3, U)


def test_small_nonforking_below():
    generate_and_check_dag(
        checks= [check_all_pairs_below],
        n_processes = 5,
        n_units = 50,
        repetitions = 30,
    )


def test_large_nonforking_below():
    generate_and_check_dag(
        checks= [check_all_pairs_below],
        n_processes = 100,
        n_units = 200,
        repetitions = 1,
    )


def test_small_forking_below():
    n_processes = 5
    generate_and_check_dag(
        checks= [check_all_pairs_below],
        n_processes = n_processes,
        n_units = 50,
        repetitions = 30,
        forking = lambda: random.randint(0, n_processes)
    )

def test_large_forking_below():
    n_processes = 100
    generate_and_check_dag(
        checks= [check_all_pairs_below],
        n_processes = n_processes,
        n_units = 200,
        repetitions = 1,
        forking = lambda: random.randint(0, n_processes)
    )


def check_all_pairs_below(dag):
    '''
    Create a poset from a dag and test (U <= V) for all pairs of units U, V against the implementation in the DAG class.
    '''
    poset, unit_dict = dag_utils.poset_from_dag(dag)

    for nodeU, U in unit_dict.items():
        for nodeV, V in unit_dict.items():
            assert poset.below(U,V) == dag.is_reachable(nodeU, nodeV), f"Problem with {nodeU} and {nodeV}"

