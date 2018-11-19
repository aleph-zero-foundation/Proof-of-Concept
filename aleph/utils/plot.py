import networkx as nx
import numpy as np
from networkx.drawing.nx_agraph import graphviz_layout

import matplotlib.pyplot as plt

from aleph.utils.dag_utils import topological_sort, get_self_predecessor

def parse_line(line):
    unit, creator_id = line.split()[:2]
    parents = line.split()[2:-1]
    self_predecessor = line.split()[-1]

    return unit, int(creator_id), parents


def pl(dag, n_processes):
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



def plot(dag, n_processes):

    G = nx.DiGraph()
    height, creator = {}, {}
    branch = {pid:{} for pid in range(n_processes)}
    self_ancestor, self_predecessor = {}, {}

    for unit, creator_id in topological_sort(dag):
        # add unit to networkx representation
        G.add_node(unit)

        # set creator[unit]
        creator[unit] = creator_id

        # set height[unit]
        height[unit] = max([height[parent] for parent, _ in dag[(unit, creator_id)]], default=-1) + 1

        for parent, parent_creator in dag[(unit, creator_id)]:
            # add edge to networkx representation
            G.add_edge(unit, parent)
            # update self_ancestor[unit]
            if parent_creator != creator_id:
                continue

        # set self_predecessor[unit]
        self_predecessor[unit] = get_self_predecessor(dag, (unit, creator_id))
        if self_predecessor[unit]:
            predecessor = self_predecessor[unit][0][0]
            self_predecessor[unit] = predecessor
            if predecessor in self_ancestor:
                self_ancestor[predecessor].append(unit)
            else:
                self_ancestor[predecessor] = [unit]

        # set branch[creator_id][unit]
        if self_predecessor[unit] is None:
            branch[creator_id][unit] = 0
        elif len(self_ancestor[self_predecessor[unit]]) == 1:
            branch[creator_id][unit] = branch[creator_id][self_predecessor[unit]]
        else:
            branch[creator_id][unit] = max(branch[creator_id].values())+1

    pos = {}

    # pos = graphviz_layout(G, prog='dot')

    # find positions of units in the plot
    # we plot units created by a given process veritcally
    # we use height[unit] for its height in the plot
    # TODO plot forks next to each other
    x = dict(zip(range(n_processes), np.linspace(27, 243, n_processes)))
    dx = x[1]-x[0]
    for pid in range(n_processes):
        units_per_pid = [unit for (unit, creator_id) in dag.keys() if creator_id == pid]
        x_per_pid = []
        heights = [height[unit] for unit in units_per_pid]
        err = 0#10 * np.random.rand(len(units_per_pid))
        spaces = 60 * np.array(heights) + err + 70
        y = dict(zip(units_per_pid, spaces))

        n_branches = len(set(branch[pid].values()))
        branch_x = np.linspace(-dx/2+5, dx/2-5, n_branches)
        for unit in units_per_pid:
            pos_y = y[unit]
            pos_x = x[creator[unit]]
            if n_branches > 1:
                pos_x += branch_x[branch[pid][unit]]

            x_per_pid.append(pos_x)
            pos[unit] = (pos_x, pos_y)

    color_values = np.linspace(0, 1, n_processes+1)[1:]
    color_map = dict(zip(range(n_processes), color_values))
    color_map[-1] = 0
    node_color = [color_map[creator[unit]] for unit in G.nodes()]
    nx.draw(G, pos, with_labels=True, arrows=True, node_color=node_color, node_size=1000, cmap=plt.get_cmap('jet'))
    plt.show()


if __name__ == '__main__':
    from aleph.utils.dag_utils import dag_from_file
    from aleph.utils.generate_poset import generate_random_nonforking, generate_random_forking
    n_processes, n_units = 4, 20
    # path = 'aleph/test/examples/random_{}_{}.txt' % (n_processes, n_units)
    # generate_random_nonforking(n_processes, n_units, path)
    n_forkers = 1
    path = 'aleph/test/examples/random_forking_{}_{}_{}.txt'.format(n_processes, n_units, n_forkers)
    generate_random_forking(n_processes, n_units, n_forkers, path)

    dag, n_processes = dag_from_file(path)
    plot(dag, n_processes)
