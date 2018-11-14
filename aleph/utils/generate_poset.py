from aleph.data_structures import Unit, Poset
from .dag_utils import is_reachable, compute_maximal_from_subset, maximal_units_per_process
import random


def create_unit_line(unit_name, process_id, parent_names):
    line = '%s %d ' % (unit_name, process_id)
    for name in parent_names:
        line = line + name + ' '
    return line
    
def generate_unit_name(unit_height, process_id, parallel_no = 0):
    if parallel_no == 0:
        name = "U_%d_%d" % (unit_height, process_id)
    else:
        name = "U_%d_%d_%d" % (unit_height, process_id, parallel_no)
    return name

def generate_random_nonforking(n_processes, n_units, file_name):
    '''
    Generates a random non-forking poset with n_processes processes and saves it to file_name. 
    Does not return any value.
    :param int n_processes: the number of processes in poset
    :param int n_units: the number of units in the process beyond genesis + n_processes initial units,
    hence the total number of units is (1 + n_processes + n_units)
    '''
    process_heights = [0] * n_processes
    file = open(file_name, 'w+')
    try:
        file.write('%d %s\n' % (n_processes, 'GENESIS'))
        for process_id in range(n_processes):
            file.write('U_0_%d %d GENESIS\n' % (process_id, process_id))
        
        for _ in range(n_units):
            process_id = random.sample(range(n_processes), 1)[0]
            all_but_process_id = list(range(n_processes))
            all_but_process_id.remove(process_id)
            parent_processes = [process_id] + random.sample(all_but_process_id , 1)
            unit_height = process_heights[process_id] + 1
            unit_name = generate_unit_name(unit_height, process_id)
            parent_names = [generate_unit_name(process_heights[id], id) for id in parent_processes]
            file.write(create_unit_line(unit_name, process_id, parent_names) + '\n')
            
            process_heights[process_id] += 1
    finally:        
        file.close()
    

def check_new_unit_correctness(dag, new_unit, new_unit_parents, forkers):
    '''
    Check whether the new unit does not introduce a diamond structure and
    whether the growth rule is preserved
    Returns the self_predecessor of new_unit if adding new_unit is correct and False otherwise
    '''
    process_id = new_unit[1]
    old_maximal_per_process = maximal_units_per_process(dag, process_id)
    
    below_per_process = []
    dag[new_unit] = new_unit_parents
    for unit in dag.keys():
        if unit[1] == process_id and unit is not new_unit and is_reachable(dag, unit, new_unit):
            below_per_process.append(unit)
    maximal_below_per_process = compute_maximal_from_subset(dag, below_per_process)
    dag.pop(new_unit, None)
    
    if len(maximal_below_per_process) !=1:
        return False
        
    self_predecessor = maximal_below_per_process[0]  
    if process_id not in forkers:
        assert len(old_maximal_per_process) == 1
        if self_predecessor != old_maximal_per_process[0]:
            return False
    
    for parent in new_unit_parents:
        if parent is not self_predecessor and is_reachable(dag, parent, self_predecessor):
            return False
    return self_predecessor
    
def count_nodes_by_process_height(node_heights, process_id, height):
    count = 0
    for node, node_height in node_heights.items():
        if node[1] == process_id and height == node_height:
            count += 1
    return count

    
def generate_random_forking(n_processes, n_units, n_forkers, file_name):
    '''
    Generates a random poset with n_processes processes, of which n_forkers are forking and saves it to file_name. 
    The growth property is guaranteed to be satsfied and there are no "diamonds" within forking processes.
    In other words, the forking processes can only create trees.
    Does not return any value.
    :param int n_processes: the number of processes in poset
    :param int n_forkers: the number of forking processes
    :param int n_units: the number of units in the process beyond genesis + n_processes initial units,
    hence the total number of units is (1 + n_processes + n_units)
    '''
    forkers = random.sample(range(n_processes), n_forkers)
    node_heights = {}
    dag = {}
    
    file = open(file_name, 'w+')
    try:
        file.write('%d %s\n' % (n_processes, 'GENESIS'))
        for process_id in range(n_processes):
            unit_name = generate_unit_name(0, process_id)
            file.write('%s %d GENESIS\n' % (unit_name, process_id))
            
            dag[(unit_name, process_id)] = []
            node_heights[(unit_name, process_id)] = 0
        

        while len(dag) < n_processes + n_units:
            process_id = random.sample(range(n_processes), 1)[0]
            new_unit_name = "temp"
            new_unit = (new_unit_name, process_id)
            new_unit_parents = random.sample(dag.keys(), 2)
            self_predecessor = check_new_unit_correctness(dag, new_unit, new_unit_parents, forkers)
            if not self_predecessor:
                continue
            new_unit_height = node_heights[self_predecessor] + 1
            new_unit_no = count_nodes_by_process_height(node_heights, process_id, new_unit_height)
            unit_name = generate_unit_name(new_unit_height, process_id, new_unit_no)
            parent_names = [parent[0] for parent in new_unit_parents]
            dag[(unit_name,process_id)] = new_unit_parents
            node_heights[(unit_name,process_id)] = new_unit_height
            file.write(create_unit_line(unit_name, process_id, parent_names) + '\n')
    finally:        
        file.close()
    
    
    
       
    

    