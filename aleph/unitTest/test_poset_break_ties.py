from aleph.utils import dag_utils
import random
from itertools import combinations


def lower_cone(poset, unit):
    ret = set([unit])
    for P in unit.parents:
        ret |= lower_cone(poset,P)
    return ret


def check_total_order_vs_below(poset, units):
    total_order = poset.break_ties(units)
    for U,V in combinations(total_order, 2):
        assert not poset.above(U,V)


def check_total_order_invariance(poset, units, n=3):
    total_order = poset.break_ties(units)
    for _ in range(n):
        random.shuffle(units)
        test = poset.break_ties(units)
        assert total_order == test


def check_break_ties_for_units(poset, units):
    check_total_order_vs_below(poset, units)
    check_total_order_invariance(poset, units)


def check_break_ties(poset):
    check_break_ties_for_units(poset, list(poset.units.values()))
    random_unit = random.choice(list(poset.units.values()))
    check_break_ties_for_units(poset, list(lower_cone(poset, random_unit)))


def test_small_nonforking_break_ties():
    random.seed(123456789)
    n_processes = 5
    n_units = 50
    repetitions = 1
    for _ in range(repetitions):
        dag = dag_utils.generate_random_nonforking(n_processes, n_units)
        poset, __ = dag_utils.poset_from_dag(dag)
        check_break_ties(poset)


def test_large_nonforking_break_ties():
    random.seed(123456789)
    n_processes = 30
    n_units = 500
    repetitions = 5
    for _ in range(repetitions):
        dag = dag_utils.generate_random_nonforking(n_processes, n_units)
        poset, __ = dag_utils.poset_from_dag(dag)
        check_break_ties(poset)


def test_large_forking_break_ties():
    random.seed(123456789)
    n_processes = 30
    n_units = 500
    repetitions = 5
    for _ in range(repetitions):
        dag = dag_utils.generate_random_forking(n_processes, n_units, 3)
        poset, __ = dag_utils.poset_from_dag(dag)
        check_break_ties(poset)

