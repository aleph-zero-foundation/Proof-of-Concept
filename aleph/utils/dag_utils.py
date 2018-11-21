'''
Several simple functions to process posets (DAGs) in a particularly simple representation.
We refer to them as to DAGs to not confuse with posets (instances of the Poset class).
A dag is represented as a dictionary: node -> (list of parent nodes).
For scenarios where node creators matter, we assume that every node is represented
as a pair (node_name, process_id), where node_name is a string name for the node
and process_id is the id of its creator.
'''

from aleph.data_structures import Poset, Unit
import random

def check_parent_diversity(dag, n_processes, treshold, U, U_parents):
    proposed_parents_processes = [node[1] for node in U_parents if node[1]!=U[1]]
    ancestor_processes = set()
    W = get_self_predecessor(dag, U, U_parents)
    while W is not None:
        new_processes = [node[1] for node in dag[W] if node[1]!=U[1]]
        ancestor_processes.update(new_processes)
        if len(ancestor_processes) >= treshold:
            return True
        if any([(pid in proposed_parents_processes) for pid in new_processes]):
            return False
        W = get_self_predecessor(dag, W, dag[W])
    return True


def check_anti_forking(dag, n_processes, U, U_parents):
    # TODO: implementation missing
    return False


def check_growth(dag, node_self_predecessor, node_parents):
    assert node_self_predecessor is not None

    for parent in node_parents:
        if parent != node_self_predecessor and is_reachable(dag, parent, node_self_predecessor):
            return False
    return True


def check_introduce_new_fork(dag, new_unit, self_predecessor):
    assert self_predecessor is not None
    process_id = new_unit[1]
    maximal_per_process = maximal_units_per_process(dag, process_id)
    return self_predecessor not in maximal_per_process




def check_new_unit_correctness(dag, new_unit, new_unit_parents, forkers):
    '''
    Check whether the new unit does not introduce a diamond structure and
    whether the growth rule is preserved
    Returns the self_predecessor of new_unit if adding new_unit is correct and False otherwise
    '''
    process_id = new_unit[1]

    self_predecessor = get_self_predecessor(dag, new_unit, new_unit_parents)

    if self_predecessor is None:
        return False

    if process_id not in forkers:
        if check_introduce_new_fork(dag, new_unit, self_predecessor):
            return False

    if not check_growth(dag, self_predecessor, new_unit_parents):
        return False

    return self_predecessor



def generate_random_nonforking(n_processes, n_units, file_name = None):
    '''
    Generates a random non-forking poset with n_processes processes and saves it to file_name.
    Does not return any value.
    :param int n_processes: the number of processes in poset
    :param int n_units: the number of units in the process beyond n_processes initial units,
    hence the total number of units is (n_processes + n_units)
    '''
    process_heights = [0] * n_processes
    dag = {}
    for process_id in range(n_processes):
        dag[('U_0_%d' % process_id, process_id)] = []

    for _ in range(n_units):
        process_id = random.sample(range(n_processes), 1)[0]
        all_but_process_id = list(range(n_processes))
        all_but_process_id.remove(process_id)
        parent_processes = [process_id] + random.sample(all_but_process_id , 1)
        unit_height = process_heights[process_id] + 1
        unit_name = generate_unit_name(unit_height, process_id)
        node = (unit_name, process_id)
        parent_nodes = [(generate_unit_name(process_heights[id], id), id) for id in parent_processes]
        dag[node] = parent_nodes
        process_heights[process_id] += 1

    if file_name:
        dag_to_file(dag, n_processes, file_name)

    return dag



def generate_random_forking(n_processes, n_units, n_forkers, file_name = None):
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
    for process_id in range(n_processes):
        unit_name = generate_unit_name(0, process_id)
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

    if file_name:
        dag_to_file(dag, n_processes, file_name)

    return dag



def generate_random_violation(n_processes, n_correct_units, n_forkers, ensure, violate):
    dag = {}
    topological_list = []

    forkers = random.sample(range(n_processes), n_forkers)
    node_heights = {}
    for process_id in range(n_processes):
        unit_name = generate_unit_name(0, process_id)
        dag[(unit_name, process_id)] = []
        topological_list.append((unit_name, process_id))
        node_heights[(unit_name, process_id)] = 0

    iter = 0
    while True:
        iter += 1
        assert iter < 1000*(n_processes + n_correct_units), "The random process had troubles to terminate."
        assert len(dag) < 100*(n_processes + n_correct_units), "The random process had troubles to terminate."

        process_id = random.sample(range(n_processes), 1)[0]
        new_unit_name = "temp"
        new_unit = (new_unit_name, process_id)
        new_unit_parents = random.sample(dag.keys(), 2)
        self_predecessor = get_self_predecessor(dag, new_unit, new_unit_parents)
        if self_predecessor is None:
            continue
        if process_id not in forkers:
            if check_introduce_new_fork(dag, new_unit, self_predecessor):
                continue

        terminate_poset = False

        property_table = {}

        property_table['growth'] = check_growth(dag, self_predecessor, new_unit_parents)
        treshold = (n_processes + 2)//3
        property_table['parent_diversity'] = check_parent_diversity(dag, n_processes, treshold,
                                                new_unit, new_unit_parents)
        property_table['anti_forking'] = check_anti_forking(dag, n_processes, new_unit, new_unit_parents)


        if len(dag) >= n_processes + n_correct_units and constraints_satisfied(violate, property_table):
            terminate_poset = True
        elif constraints_satisfied(ensure, property_table):
            pass
        else:
            #cannot add this node to graph
            continue


        new_unit_height = node_heights[self_predecessor] + 1
        new_unit_no = count_nodes_by_process_height(node_heights, process_id, new_unit_height)
        unit_name = generate_unit_name(new_unit_height, process_id, new_unit_no)
        parent_names = [parent[0] for parent in new_unit_parents]
        topological_list.append((unit_name,process_id))
        dag[(unit_name,process_id)] = new_unit_parents
        node_heights[(unit_name,process_id)] = new_unit_height
        if terminate_poset:
            break

    return dag, topological_list

#======================================================================================================================



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



def reversed_dag(dag):
    rev_dag = {node : [] for node in dag.keys()}
    for node, parents in dag.items():
        for parent_node in parents:
            rev_dag[parent_node].append(node)
    return rev_dag


#======================================================================================================================


def poset_from_dag(dag, n_processes):

    unit_dict = {}
    poset_from_dag = Poset(n_processes = n_processes, process_id = 0,
                            secret_key = None, public_key = None)

    topological_list = topological_sort(dag)
    for (unit_name, unit_creator_id) in topological_list:
        node = (unit_name, unit_creator_id)
        assert 0 <= unit_creator_id <= n_processes - 1, "Incorrect process id"
        parents = dag[node]
        assert unit_name not in unit_dict.keys(), "Duplicate unit name %s" % unit_name
        for parent in parents:
            assert parent[0] in unit_dict.keys(), "Parent %s of a unit %s not known" % (parent, unit_name)

        U = Unit(creator_id = unit_creator_id, parents = [unit_dict[parent[0]] for parent in parents],
                txs = [])
        poset_from_dag.add_unit(U)
        unit_dict[unit_name] = U

    return poset_from_dag, unit_dict



def dag_to_file(dag, n_processes, file_name):
    topological_list = topological_sort(dag)
    with open(file_name, 'w') as f:
        f.write('%d\n' % n_processes)
        for node in topological_list:
            parent_nodes = dag[node]
            line = create_node_line(node[0], node[1], [parent_node[0] for parent_node in parent_nodes])
            f.write(line+'\n')



def dag_from_file(file_name):
    with open(file_name) as poset_file:
        lines = poset_file.readlines()

    dag = {}
    name_to_process_id = {}
    head_line = lines[0]
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

    return dag, n_processes



def create_node_line(node_name, process_id, parent_names):
    line = '%s %d ' % (node_name, process_id)
    for name in parent_names:
        line = line + name + ' '
    return line



def generate_unit_name(unit_height, process_id, parallel_no = 0):
    if parallel_no == 0:
        name = "U_%d_%d" % (unit_height, process_id)
    else:
        name = "U_%d_%d_%d" % (unit_height, process_id, parallel_no)
    return name


#======================================================================================================================


def get_self_predecessor(dag, node, parent_nodes):
    pid = node[1]
    below_within_process = [node_below for node_below in nodes_below_set(dag, parent_nodes) if node_below[1] == pid]

    if len(below_within_process) == 0:
        return None
    else:
        list_maximal = compute_maximal_from_subset(dag, below_within_process)
        if len(list_maximal) != 1:
            return None
        return list_maximal[0]



def count_nodes_by_process_height(node_heights, process_id, height):
    count = 0
    for node, node_height in node_heights.items():
        if node[1] == process_id and height == node_height:
            count += 1
    return count



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



def is_reachable_through_n_intermediate(dag, U, V, n_intermediate):
    '''
    Checks whether the number of differente processes that have units on some path
    from U to V in dag is at least n_intermediate.
    '''
    nodes_below_V = nodes_below(dag, V)
    nodes_above_U = nodes_below(reversed_dag(dag), U)

    processes_on_paths = []

    for node in nodes_below_V:
        if node in nodes_above_U:
            processes_on_paths.append(node[1])

    n_processes_on_paths = len(set(processes_on_paths))
    return n_processes_on_paths >= n_intermediate

def nodes_below_set(dag, set_of_nodes):
    '''
    Finds the set of all nodes U that have a path from U to one of nodes in set_of_nodes.
    '''
    have_path = set([])
    node_head = set(set_of_nodes)
    while node_head:
        node  = node_head.pop()
        if node not in have_path:
            have_path.add(node)
            for parent_node in dag[node]:
                if parent_node in have_path:
                    continue
                node_head.add(parent_node)

    return list(have_path)


def nodes_below(dag, V):
    '''
    Finds the set of all nodes U that have a path from U to V.
    '''
    return nodes_below_set(dag, [V])



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


def constraints_satisfied(constraints, truth):
    for constraint, value in constraints.items():
        if truth[constraint] != value:
            return False
    return True










