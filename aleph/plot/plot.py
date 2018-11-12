import networkx as nx
from networkx.drawing.nx_agraph import write_dot, graphviz_layout

import matplotlib.pyplot as plt
import matplotlib.cbook
import warnings
warnings.filterwarnings("ignore", category=matplotlib.cbook.mplDeprecation)


'''
edges = (network > thr).astype(int)
edges -= (network < -thr).astype(int)

G = nx.from_numpy_matrix(edges)

pos = positions[:,[0,1]]

nx.draw_networkx_nodes(G, pos, node_size = 2, ax = ax[band//3 ,band%3], node_color = 'black')

G = nx.from_numpy_matrix(edges == 1)
nx.draw_networkx_edges(G, pos, edge_color = 'red',  ax = ax[band//3 ,band%3], width = 0.1)

pos=graphviz_layout(G, prog='dot')
plt.figure(figsize=(30,30))
nx.draw(G, pos, with_labels=True, arrows=False, hold=True, node_size=1000)
plt.savefig('mcts')
'''


def plot(path):
    f = open(path, 'r')

    n_processes, genesis_unit = f.readline().split(',')[:2]
    genesis_unit = genesis_unit[:-1]
    n_processes = int(n_processes)

    G = nx.DiGraph()
    G.add_node(genesis_unit)

    for line in f:
        line = line[:-1]
        node, creator_id = line.split(',')[:2]
        parents = line.split(',')[2:]
        print(node, creator_id, parents)
        G.add_node(node)
        for parent in parents:
            G.add_edge(node, parent)

    f.close()

    pos = graphviz_layout(G, prog='dot')
    plt.figure(figsize=(30,30))
    nx.draw(G, pos, with_labels=True, arrows=True, hold=True, node_size=1000)
    plt.show()

if __name__ == '__main__':
    plot('simple_graph')
