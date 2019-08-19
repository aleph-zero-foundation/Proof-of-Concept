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
from aleph.utils.generic_test import generate_and_check_dag
import random
from itertools import combinations


def lower_cone(poset, U):
    '''
    :param Poset poset: the considered poset
    :param Unit U: the unit whose lower cone is to be computed
    :returns: The set of all units that are below U in poset.
    '''
    ret = set([U])
    for P in U.parents:
        ret |= lower_cone(poset,P)
    return ret


def check_total_order_vs_below(poset, units):
    '''
    Tests whether the linear ordering output by break_ties is compatible with the poset.
    :param Poset poset: the considered poset
    :param list units: a list of units to be tested
    '''
    total_order = poset.break_ties(units)
    for U,V in combinations(total_order, 2):
        assert not poset.above(U,V)


def check_total_order_invariance(poset, units, repetitions=3):
    '''
    Tests whether the linear ordering output by break_ties does not depend on the initial ordering of the input.
    :param Poset poset: the considered poset
    :param list units: a list of units to be tested
    :param int repetitions: the number of tests to perform
    '''
    total_order = poset.break_ties(units)
    for _ in range(repetitions):
        random.shuffle(units)
        test = poset.break_ties(units)
        assert total_order == test


def check_break_ties_for_units(poset, units):
    '''
    Run the correctness tests of break ties on a given poset for a given set of units.
    :param Poset poset: the considered poset
    :param list units: a list of units to be tested
    '''
    check_total_order_vs_below(poset, units)
    check_total_order_invariance(poset, units)


def check_break_ties(dag):
    '''
    Takes a dag as input, turns it into a poset and runs tests for break ties:
        - on all units,
        - on the lower-cone of a random unit.
    :param DAG dag: a dag to run the test
    '''
    poset, __ = dag_utils.poset_from_dag(dag)
    check_break_ties_for_units(poset, list(poset.units.values()))
    random_unit = random.choice(list(poset.units.values()))
    check_break_ties_for_units(poset, list(lower_cone(poset, random_unit)))


def test_small_nonforking_break_ties():
    generate_and_check_dag(
        checks= [check_break_ties],
        n_processes = 5,
        n_units = 50,
        repetitions = 20,
    )


def test_large_nonforking_break_ties():
    generate_and_check_dag(
        checks= [check_break_ties],
        n_processes = 30,
        n_units = 500,
        repetitions = 1,
    )


def test_large_forking_break_ties():
    generate_and_check_dag(
        checks= [check_break_ties],
        n_processes = 30,
        n_units = 500,
        repetitions = 1,
        forking = lambda: 3
    )
