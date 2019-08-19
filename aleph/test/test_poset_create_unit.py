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

def test_create_unit_small():
    random.seed(123456789)
    repetitions = 50
    for rep in range(repetitions):
        n_processes = random.randint(4, 15)
        n_units = random.randint(0, n_processes*5)
        simulate_with_checks(
                n_processes,
                n_units,
            )


def test_create_unit_large():
    random.seed(123456789)
    repetitions = 5
    for rep in range(repetitions):
        n_processes = random.randint(30, 80)
        n_units = random.randint(0, n_processes*3)
        simulate_with_checks(
                n_processes,
                n_units,
            )
