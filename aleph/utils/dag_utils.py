'''
Several simple functions to process posets (DAGs) in a particularly simple representation.
We refer to them as to DAGs to not confuse with posets (instances of the Poset class).
A dag is represented as a dictionary: node -> (list of parent nodes).
For scenarios where node creators matter, we assume that every node is represented 
as a pair (node_name, process_id), where node_name is a string name for the node
and process_id is the id of its creator.
'''
from aleph.data_structures import Poset, Unit
    


def is_reachable(dag, U, V):
    '''Checks whether V is reachable from U in a DAG, using BFS
    :param dict dag: a dictionary of the form: node -> [list of parent nodes]
    :returns: a boolean value True if reachable, False otherwise
    '''
    have_path_to_V = set([])
    node_head = set([V])
    while node_head:
        node  = node_head.pop()
        if node == U:
            return True
        
        if node not in have_path_to_V:
            have_path_to_V.add(node)
            for parent_node in dag[node]:
                if parent_node in have_path_to_V:
                    continue
                node_head.add(parent_node)
                
        have_path_to_V.add(node)
    return False
    
    
def compute_maximal_from_subset(dag, subset):
    maximal_from_subset = []
    for U in subset:
        is_maximal = True
        for V in subset:
            if V is not U and is_reachable(dag, U, V):
                is_maximal = False
                break
        if is_maximal:
            maximal_from_subset.append(U)
    return maximal_from_subset

    
def maximal_units_per_process(dag, process_id):
    units_per_process = []
    for U in dag.keys():
        if U[1] == process_id:
            units_per_process.append(U)
            
    maximal_units = compute_maximal_from_subset(dag, units_per_process)
           
    return maximal_units
    
    
    