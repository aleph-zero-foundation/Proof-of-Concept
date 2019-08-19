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

from aleph.utils import dag_utils
import log_analyzer
import math
import matplotlib.pyplot as plt
import os
import random
import sys


def get_popularity_stats(dag, stop_criteria=lambda x, y: True, initial_level=3):
    '''
    Generates statistics describing after how many levels a prime unit becomes popular.
    '''
    popularity_stats = {}
    unpopular_primes = set(dag.get_prime_units_by_level(initial_level))
    level = initial_level + 1
    primes = dag.get_prime_units_by_level(level)
    while primes and stop_criteria(primes, level):

        now_popular = set(unpopular_primes)
        for U in primes:
            now_popular = set(V for V in now_popular if dag.is_reachable(V, U))
            if not now_popular:
                break

        for U in now_popular:
            time_to_popular = level - dag.level(U)
            stat = popularity_stats.get(time_to_popular, 0)
            popularity_stats[time_to_popular] = stat + 1

        unpopular_primes -= now_popular

        unpopular_primes |= set(primes)
        level += 1
        primes = dag.get_prime_units_by_level(level)

    return popularity_stats


def get_prime_units_stats(dag):
    '''
    Generates for each level statistics describing properties of prime units, i.e. number of prime units, number of minimal
    prime units and statistics describing for every prime unit the number of prime units reachable at lower level.
    '''
    level = 0
    primes = dag.get_prime_units_by_level(level)
    while primes:

        min_primes = sum(1 for U in primes if all(dag.level(V) < dag.level(U) for V in dag.parents(U)))

        all_visible_below = []
        for U in primes:
            primes_below = dag.get_prime_units_by_level(level-1)
            cnt = sum(dag.is_reachable(V, U) for V in primes_below)
            all_visible_below.append(cnt)

        yield level, len(primes), min_primes, all_visible_below
        level += 1
        primes = dag.get_prime_units_by_level(level)


def get_units_per_process_per_level(dag, units_to_analyze):
    '''
    Generates statistics grouping number of created units per level and per process.
    '''
    level_stats = {}
    max_level = 0
    for unit in units_to_analyze:
        level = dag.level(unit)
        max_level = max(max_level, level)
        pid = dag.pid(unit)
        pid_stats = level_stats.get(pid, {})
        level_count = pid_stats.get(level, 0)
        pid_stats[level] = level_count + 1
        level_stats[pid] = pid_stats
    return level_stats, max_level


def plot_series(plot_file, x_series, y_series):
    plt.bar(x_series[0], y_series[0])
    plt.xlabel(x_series[1])
    plt.ylabel(y_series[1])
    plt.savefig(plot_file, dpi=800)
    plt.close()


def print_primes_stats(dag, remove_first=1, remove_last=1):
    '''
    Prints on standard output statistics describing properties of prime units. See description of <get_prime_units_stats> method
    for more details.
    '''
    all_visible_below = []
    primes_per_level = []

    print("level   (pr. units)   (min pr. units)   (avg n vis. below)")

    for level, primes_count, min_primes_count, visible_below in get_prime_units_stats(dag):
        all_visible_below.append(visible_below)
        primes_per_level.append(primes_count)
        avg_visible_below = sum(visible_below)/primes_count
        print(f'{level: <5}   {primes_count: <11}   {min_primes_count: <15}   {avg_visible_below:<18.2f}')
    primes_per_level = primes_per_level[remove_first:-remove_last]
    p_stats = log_analyzer.compute_basic_stats(primes_per_level)

    print(f'prime units: min={p_stats["min"]}, max={p_stats["max"]}, avg={p_stats["avg"]:.2f}, '
          f'# of samples={p_stats["n_samples"]}')

    all_visible_below = all_visible_below[1:]
    all_visible_below = [count for level in all_visible_below for count in level]
    pa_stats = log_analyzer.compute_basic_stats(all_visible_below)

    print(f'prime ancestors: min={pa_stats["min"]}, max={pa_stats["max"]}, avg={pa_stats["avg"]:.2f}, '
          f'# of samples={pa_stats["n_samples"]}')


def print_units_stats_per_level(dag, plot_output, pid_filter=lambda x: True, remove_first=1, remove_last=1):
    '''
    Prints to standard output statistics describing number of units created per level.
    '''
    units_to_analyze = dag.get_node_list_as_added()

    level_stats, max_level = get_units_per_process_per_level(dag, units_to_analyze)
    units_per_level = [0] * (max_level + 1)
    for pid, level_counts in sorted(level_stats.items()):
        process_it = pid_filter(pid)
        if process_it:
            print(f'Process {pid:d}')
            print('level   units on level')
        pid_levels = [0] * (max_level + 1)
        skipped_levels = [0] * (max_level + 1)
        current_skip = 0
        for level in range(max_level + 1):
            units_created = level_counts.get(level, 0)
            units_per_level[level] += units_created
            pid_levels[level] = units_created
            if process_it:
                print(f'{level:>5d}   {units_created}')
            if level > 0:
                if units_created == 0:
                    current_skip += 1
                else:
                    skipped_levels[level] = current_skip
                    current_skip = 0
        if process_it:
            result_stats = log_analyzer.compute_basic_stats(pid_levels)
            skipped_stats = log_analyzer.compute_basic_stats(skipped_levels)

            print(f'units per level: min={result_stats["min"]}, max={result_stats["max"]}, avg={result_stats["avg"]:.2f}')
            print(f'skipped levels: min={skipped_stats["min"]}, max={skipped_stats["max"]}')
            print()

    units_per_level = units_per_level[remove_first:-remove_last]

    print('Overall number of units per level')
    print('level   # of units   avg')

    for level, units_count in enumerate(units_per_level):
        print(f'{level+remove_first:>5d}   {units_count:10d}   {units_count/len(level_stats):.2f}')

    process_count = len(level_stats)
    units_per_level = units_per_level[remove_first:-remove_last]
    level_stats = log_analyzer.compute_basic_stats(units_per_level)

    print(f'units per level: min_to_proceed={math.ceil(2*process_count/3)},  min={level_stats["min"]}, '
          f'max={level_stats["max"]}, avg={level_stats["avg"]:.2f}')

    if plot_output:
        plot_series(plot_output, (range(len(units_per_level)), "level"), (units_per_level, "units created"))


def print_popularity_stats(dag):
    '''
    Prints to standard output statistics describing how fast prime units are becoming popular.
    '''
    popularity_stats = get_popularity_stats(dag)

    print('+levels to popularity     count')

    for level, count in sorted(popularity_stats.items()):
        print(f'{level:21d}     {count:d}')


def print_help():
    print("Usage: python dumped_poset_analyzer.py <dag file> <output_dir:optional>")


if __name__ == '__main__':
    file_name = 'poset.dag'
    dest_dir = None
    level_plot_file = None

    if len(sys.argv) < 2:
        print_help()
        exit(-1)

    file_name = sys.argv[1]
    if not os.path.exists(file_name):
        print(f"The file {file_name} doesn't exist. Aborting.")
        print_help()
        exit(-1)

    if len(sys.argv) == 3:
        dest_dir = sys.argv[2]
        if not os.path.exists(dest_dir):
            print(f"The output directory {dest_dir} doesn't exist. Aborting.")
            print_help()
            exit(-1)
        level_plot_file = os.path.join(dest_dir, 'levels.png')

    dag = dag_utils.dag_from_file(file_name)
    print(f"Dag consists of {len(dag)} units.")
    print()

    random_process = random.randrange(dag.n_processes)
    print('Statistics describing number of units created per level')
    print('Presenting data describing a randomly selected process')
    print_units_stats_per_level(dag, level_plot_file, lambda x: x == random_process)
    print()

    print('Statistics describing properties of prime units per level')
    print_primes_stats(dag)
    print()

    print('Statistics describing number of levels required for a prime unit to become popular')
    print_popularity_stats(dag)
