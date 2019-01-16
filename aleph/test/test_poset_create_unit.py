from aleph.utils.generic_test import simulate_with_checks
import random

def test_create_unit_small():
    random.seed(123456789)
    repetitions = 50
    for strategy in ["link_self_predecessor", "link_above_self_predecessor"]:
        for rep in range(repetitions):
            n_processes = random.randint(4, 15)
            n_units = random.randint(0, n_processes*5)
            n_forkers = random.randint(0,n_processes//3)
            simulate_with_checks(
                    n_processes,
                    n_units,
                    n_forkers,
                    strategy,
                    only_maximal_parents = False,
                )


def test_create_unit_large():
    random.seed(123456789)
    repetitions = 5
    for strategy in ["link_self_predecessor", "link_above_self_predecessor"]:
        for rep in range(repetitions):
            n_processes = random.randint(30, 80)
            n_units = random.randint(0, n_processes*3)
            n_forkers = random.randint(0,n_processes//3)
            simulate_with_checks(
                    n_processes,
                    n_units,
                    n_forkers,
                    strategy,
                    only_maximal_parents = False,
                )
