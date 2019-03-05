from aleph.utils import dag_utils
from aleph.utils.generic_test import generate_and_check_dag
import random



def test_small_nonforking_below():
    generate_and_check_dag(
        checks= [check_all_levels],
        n_processes = 5,
        n_units = 50,
        repetitions = 1,
    )

def test_large_nonforking_below():
    generate_and_check_dag(
        checks= [check_all_levels],
        n_processes = 30,
        n_units = 100,
        repetitions = 1,
    )


def test_small_forking_below():
    generate_and_check_dag(
        checks= [check_all_levels],
        n_processes = 5,
        n_units = 50,
        repetitions = 30,
        forking = lambda: 1
    )


def test_large_forking_below():
    generate_and_check_dag(
        checks= [check_all_levels],
        n_processes = 30,
        n_units = 100,
        repetitions = 10,
        forking = lambda: 2
    )

def test_specific_dag():
    from pkg_resources import resource_stream
    check_all_levels(dag_utils.dag_from_stream(resource_stream("aleph.test.data", "simple.dag")))

def check_all_levels(arg):
    '''
    Create a poset from a dag and check if levels agree.
    '''
    poset, unit_dict = dag_utils.poset_from_dag(arg)

    for nodeU, U in unit_dict.items():
        assert U.level == arg.levels[nodeU], f"Node {nodeU} has broken level!"
