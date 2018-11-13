import networkx as nx
import numpy as np
from networkx.drawing.nx_agraph import graphviz_layout

import matplotlib.pyplot as plt


def parse_line(line):
    node, creator_id = line.split()[:2]
    parents = line.split()[2:]

    return node, int(creator_id), parents

def plot(path):
    '''
    :param string path: path to a file with poset description
    '''
    unit_creator = {}

    n_processes, genesis_unit = None, None
    x, units_per_process = None, None
    G = nx.DiGraph()


    with open(path, 'r') as f:

        n_processes, genesis_unit = f.readline().split()
        n_processes = int(n_processes)
        G.add_node(genesis_unit)
        unit_creator[genesis_unit] = -1
        units_per_process = [[] for _ in range(n_processes)]
        x = dict(zip(range(n_processes), np.linspace(27, 243, n_processes)))

        for line in f:
            node, creator_id, parents = parse_line(line)

            unit_creator[node] = creator_id
            units_per_process[creator_id].append(node)

            G.add_node(node)
            for parent in parents:
                G.add_edge(node, parent)

    #pos = graphviz_layout(G, prog='dot')
    # plt.figure(figsize=(30, 30))

    pos = {'G':(135,10)}
    for pid in range(n_processes):
        err = 10 * np.random.rand(len(units_per_process[pid]))
        spaces = 60*np.arange(len(units_per_process[pid])) + err + 70
        print(spaces)
        y = dict(zip(units_per_process[pid], spaces))

        for unit in units_per_process[pid]:
            pos[unit] = (x[unit_creator[unit]], y[unit])

    color_values = np.linspace(0, 1, n_processes+1)[1:]
    color_map = dict(zip(range(n_processes), color_values))
    color_map[-1] = 0
    print(color_map)
    node_color = [color_map[unit_creator[node]] for node in G.nodes()]
    nx.draw(G, pos, with_labels=True, arrows=True, node_color=node_color)
    plt.show()


if __name__ == '__main__':
    plot('simple_graph')
