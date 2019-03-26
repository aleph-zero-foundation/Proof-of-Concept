import random
from time import time
from joblib import Parallel, delayed

from aleph.utils.generic_test import simulate_with_checks

def time_coin_toss(U, poset, dag, results, additional_args):
    if additional_args is None:
        levels, U_c = set(), None
    else:
        levels, U_c = additional_args
    if U.level not in levels:
        levels.add(U.level)
        if U.level == 3:
            U_c = U
        if U_c is not None and U.level-U_c.level >= 4:
            start = time()
            poset.toss_coin(U_c, U)
            end = time()-start
            results.append(end)
    return levels, U_c

def measure_time(n_jobs, n_processes, n_units, n_forkers):
    print('n_jobs', n_jobs, '\nn_processes', n_processes, '\nn_units', n_units, '\nn_forkers', n_forkers)
    print('dispatching workers')
    start = time()
    results = Parallel(n_jobs=n_jobs)(
        delayed(simulate_with_checks)(
            n_processes,
            n_units,
            n_forkers,
            post_prepare = time_coin_toss,
            seed = round(time())+i
        ) for i in range(n_jobs))
    delta = time()-start
    if delta < 60:
        print('work done in', round(delta, 2))
    else:
        print('work done in', round(delta/60, 2))
    all_sum, all_len = 0, 0
    for res in results:
        all_sum += sum(res)
        all_len += len(res)
    print(all_len, all_sum/all_len, [len(res) for res in results])

if __name__ == '__main__':
    measure_time(8, 4, 1000, 0)
    measure_time(8, 100, 50000, 0)
