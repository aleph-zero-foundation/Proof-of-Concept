from aleph.utils.generic_test import simulate_with_checks
import random

def print_level_at_prime(U, poset, dag, results, additional_args):
    if additional_args is None:
        levels = set()
    else:
        levels = additional_args
    if U.level not in levels:
        levels.add(U.level)
        print(U.level, len(dag))

def run(repetitions, min_proc, max_proc, units_per_process):
    random.seed(123456789)
    for rep in range(repetitions):
        n_processes = random.randint(min_proc, max_proc)
        n_units = random.randint(0, units_per_process)
        n_forkers = random.randint(0,n_processes//3)
        print('test setting', rep, n_processes, n_units, n_forkers)
        print('level, size')
        simulate_with_checks(
                n_processes,
                n_units,
                n_forkers,
                post_prepare = print_level_at_prime
            )


def test_level():
    run(10, 6, 6, 300)
