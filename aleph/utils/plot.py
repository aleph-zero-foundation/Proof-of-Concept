import networkx as nx
import numpy as np
from networkx.drawing.nx_agraph import graphviz_layout

import matplotlib.pyplot as plt


def parse_line(line):
    unit, creator_id = line.split()[:2]
    parents = line.split()[2:-1]
    self_predecessor = line.split()[-1]

    return unit, int(creator_id), parents


def plot(path):
    '''
    :param string path: path to a file with poset description
    Reads a description of a poset stored in path and draws a simple plot.
    '''
    creator, height = {}, {}
    self_ancestor, self_predecessor = {}, {}

    n_processes, genesis_unit = None, None
    units_per_process = None
    G = nx.DiGraph()
    branch = None

    with open(path, 'r') as f:
        n_processes, genesis_unit = f.readline().split()
        n_processes = int(n_processes)

        G.add_node(genesis_unit)
        height[genesis_unit] = -1
        creator[genesis_unit] = -1

        units_per_process = [[] for _ in range(n_processes)]
        branch = {pid:{} for pid in range(n_processes)}
        # set x coordinates of units per process

        for line in f:
            unit, creator_id, parents, self_predecessor  = parse_line(line)

            creator[unit] = creator_id
            units_per_process[creator_id].append(unit)

            self_predecessor[unit] = self_predecessor
            for parent in parents:
                if parent in self_ancestor:
                    self_ancestor[parent].append(unit)
                else:
                    self_ancestor[parent] = [unit]

            if len(units_per_process[creator_id]) == 1:
                branch[creator_id][unit] = 0
                self_predecessor[unit] = []
            elif len(self_ancestor[self_predecessor[unit]]) == 1:
                branch[creator_id][unit] = branch[creator_id][self_predecessor[unit]]
            else:
                branch[creator_id][unit] = max(branch[creator_id].values())+1

            height[unit] = max([height[parent] for parent in parents]) + 1

            G.add_node(unit)
            for parent in parents:
                G.add_edge(unit, parent)

    # pos = graphviz_layout(G, prog='dot')

    # find positions of units in the plot
    # we plot units created by a given process veritcally
    # we use height[unit] for its height in the plot
    # TODO plot forks next to each other
    x = dict(zip(range(n_processes), np.linspace(27, 243, n_processes)))
    dx = x[1]-x[0]
    pos = {genesis_unit: (135, 10)}
    for pid in range(n_processes):
        heights = [height[unit] for unit in units_per_process[pid]]
        err = 10 * np.random.rand(len(units_per_process[pid]))
        spaces = 60*np.array(heights) + err + 70
        y = dict(zip(units_per_process[pid], spaces))
        
        n_branches = len(branch[pid].values())
        branch_x = np.linspace(-dx/2+5, dx/2-5, n_branches)
        for unit in units_per_process[pid]:
            pos_y = y[unit]
            pos_x = x[creator[unit]]
            if n_branches > 1:
                pos_x += branch_x[branch[pid][unit]]

            pos[unit] = (pos_x, pos_y)

    color_values = np.linspace(0, 1, n_processes+1)[1:]
    color_map = dict(zip(range(n_processes), color_values))
    color_map[-1] = 0
    node_color = [color_map[creator[unit]] for unit in G.nodes()]
    nx.draw(G, pos, with_labels=True, arrows=True, node_color=node_color, node_size=1000, cmap=plt.get_cmap('jet'))
    plt.show()


if __name__ == '__main__':
    plot('random_forking_6_20.txt')
    # plot('simple_graph')
