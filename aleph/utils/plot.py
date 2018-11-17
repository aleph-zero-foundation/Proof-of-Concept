import networkx as nx
import numpy as np
from networkx.drawing.nx_agraph import graphviz_layout

import matplotlib.pyplot as plt

from aleph.utils.dag_utils import topological_sort

def parse_line(line):
    unit, creator_id = line.split()[:2]
    parents = line.split()[2:-1]
    self_predecessor = line.split()[-1]

    return unit, int(creator_id), parents


def plot(dag, n_processes):
    '''
    :param dict dag: (unit, creator_id) -> [unit_parents]
    '''

    G = nx.DiGraph()
    height, creator = {}, {}

    for unit, creator_id in topological_sort(dag):
        G.add_node(unit)
        for parent, _ in dag[(unit, creator_id)]:
            G.add_edge(unit, parent)
        height[unit] = max([height[parent] for parent, _ in dag[(unit, creator_id)]], default=-1) + 1
        creator[unit] = creator_id

    pos = {}

    x = dict(zip(range(n_processes), np.linspace(27, 243, n_processes)))
    for pid in range(n_processes):
        units_per_pid = [unit for (unit, creator_id) in dag.keys() if creator_id == pid]
        err = 10 * np.random.rand(len(units_per_pid))
        spaces = 60*np.array([height[unit] for unit in units_per_pid]) + err + 70
        y = dict(zip(units_per_pid, spaces))

        for unit in units_per_pid:
            pos[unit] = (x[pid], y[unit])

    color_values = np.linspace(0, 1, n_processes+1)[1:]
    color_map = dict(zip(range(n_processes), color_values))
    node_color = [color_map[creator[unit]] for unit in G.nodes()]
    nx.draw(G, pos, with_labels=True, arrows=True, node_color=node_color, node_size=1000, cmap=plt.get_cmap('jet'))
    plt.show()


if __name__ == '__main__':
    from aleph.utils.dag_utils import dag_from_file
    # from aleph.utils.generate_poset import generate_random_nonforking
    path = 'aleph/test/examples/random_10_30.txt'
    # generate_random_nonforking(10, 30, path)
    dag, n_processes = dag_from_file(path)
    plot(dag, n_processes)
