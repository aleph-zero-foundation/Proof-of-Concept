'''
Several simple functions to process posets (DAGs) in a particularly simple representation.
We refer to them as to DAGs to not confuse with posets (instances of the Poset class).
A dag is represented as a dictionary: node -> (list of parent nodes).
For scenarios where node creators matter, we assume that every node is represented 
as a pair (node_name, process_id), where node_name is a string name for the node
and process_id is the id of its creator.
'''

from aleph.data_structures import Poset, Unit
    
    
def _create_node_line(node_name, process_id, parent_names):
    line = '%s %d ' % (node_name, process_id)
    for name in parent_names:
        line = line + name + ' '
    return line    
    
def topological_sort(dag):
    '''
    slow, iterative implementation of topological sort of a dag
    '''
    nodes_included = set()
    topological_list = []
    iterations = 0
    while len(nodes_included)<len(dag):
        iterations += 1
        assert iterations <= len(dag)+1, "The input dag seems to have loops or undefined nodes."
        for node, parent_nodes in dag.items():
            if node in nodes_included:
                continue
            all_parents_included = True
            for parent_node in dag[node]:
                if parent_node not in nodes_included:
                    all_parents_included = False
                    break
            if all_parents_included:
                topological_list.append(node)
                nodes_included.add(node)
    return list(topological_list)
    
def dag_to_file(dag, n_processes, file_name):
    topological_list = topological_sort(dag)
    with open(file_name, 'w') as f:
        f.write('%d\n' % n_processes)
        for node in topological_list:
            parent_nodes = dag[node]
            line = _create_node_line(node[0], node[1], [parent_node[0] for parent_node in parent_nodes])
            f.write(line+'\n')

    
    
def dag_from_file(file_name):
    with open(file_name) as poset_file:
        lines = poset_file.readlines()
    
    dag = {}
    name_to_process_id = {}
    n_processes = int(head_line)

    for line in lines[1:]:
        tokens = line.split()
        unit_name = tokens[0]
        unit_creator_id = int(tokens[1])
        assert 0 <= unit_creator_id <= n_processes - 1, "Incorrect process id"
        parents = tokens[2:]
        assert unit_name not in name_to_process_id.keys(), "Duplicate unit name %s" % unit_name
        for parent in parents:
            assert parent in name_to_process_id.keys(), "Parent %s of a unit %s not known" % (parent, unit_name)

        dag_parents = [(name, name_to_process_id[name]) for name in parents]
        dag[(unit_name, unit_creator_id)] = dag_parents
        name_to_process_id[unit_name] = unit_creator_id

    return dag

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
    
    
    