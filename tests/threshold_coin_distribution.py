'''
    This is a Proof-of-Concept implementation of Aleph Zero consensus protocol.
    Copyright (C) 2019 Aleph Zero Team
    
    This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
    This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
    
    You should have received a copy of the GNU General Public License
    along with this program. If not, see <http://www.gnu.org/licenses/>.
'''

from aleph.utils.generic_test import simulate_with_checks
import random
from joblib import Parallel, delayed
from time import time

def report_coin_toss(U, poset, dag, results, additional_args):
    if additional_args is None:
        levels, U_c = set(), None
    else:
        levels, U_c = additional_args
    if U.level not in levels:
        levels.add(U.level)
        if U.level == 3:
            U_c = U
        if U_c is not None and U.level-U_c.level >= 4:
            results.append(poset.toss_coin(U_c, U))
    return levels, U_c


def check_distribution(n_jobs, n_processes, n_units, n_forkers):
    print('n_jobs', n_jobs, '\nn_processes', n_processes, '\nn_units', n_units, '\nn_forkers', n_forkers)
    print('dispatching workers')
    start = time()
    results = Parallel(n_jobs=n_jobs)(
        delayed(simulate_with_checks)(
            n_processes,
            n_units,
            n_forkers,
            post_prepare = report_coin_toss,
            use_tcoin = True,
            seed = round(time())+i
        ) for i in range(n_jobs))
    print('work done in', round(time()-start,2))

    all_sum, all_len = 0, 0
    for res in results:
        all_sum += sum(res)
        all_len += len(res)
    if all_len:
        print(all_len, all_sum/all_len)


if __name__ == '__main__':
    check_distribution(8, 16, 4000, 0)
