import networkx as nx
from networkx.drawing.nx_agraph import graphviz_layout

import matplotlib.pyplot as plt


def parse_line(line):
    node, creator_id = line.split()[:2]
    parents = line.split()[2:]

    return node, creator_id, parents

def plot(path):
    '''
    :param string path: path to a file with poset description
    '''
    with open(path, 'r') as f:

        n_processes, genesis_unit = f.readline().split()
        n_processes = int(n_processes)

        G = nx.DiGraph()
        G.add_node(genesis_unit)

        for line in f:
            node, creator_id, parents = parse_line(line)
            G.add_node(node)
            for parent in parents:
                G.add_edge(node, parent)

    pos = graphviz_layout(G, prog='dot')
    plt.figure(figsize=(30, 30))
    nx.draw(G, pos, with_labels=True, arrows=True, hold=True, node_size=1000)
    plt.show()

if __name__ == '__main__':
    plot('simple_graph')
