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
