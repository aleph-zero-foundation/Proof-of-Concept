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

import sys

from aleph.utils.dag_utils import dag_from_file, generate_random_forking, generate_random_nonforking
from aleph.utils.plot import plot_dag

path = ""
if len(sys.argv) == 2:
    path = sys.argv[1]
elif len(sys.argv) == 3:
    n_processes, n_units = int(sys.argv[1]), int(sys.argv[2])
    path = 'random_nonforking_{}_{}.txt'.format(n_processes, n_units)
    generate_random_nonforking(n_processes, n_units, path)
elif len(sys.argv) == 4:
    n_processes, n_units, n_forkers = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
    path = 'random_forking_{}_{}_{}.txt'.format(n_processes, n_units, n_forkers)
    generate_random_forking(n_processes, n_units, n_forkers, path)
else:
    print("Either provide a file to plot or number of processes, units and (optionally) forkers.")
    sys.exit(1)
dag = dag_from_file(path)
plot_dag(dag)
