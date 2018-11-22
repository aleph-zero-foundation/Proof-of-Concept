import random
from .dag import DAG, dag_to_file


def check_parent_diversity(dag, pid, parents, threshold):
    proposed_parents_processes = [dag.pid(node) for node in parents if dag.pid(node) != pid]
    ancestor_processes = set()
    W = dag.self_predecessor(pid, parents)
    while W is not None:
        new_processes = [dag.pid(node) for node in dag.parents(W) if dag.pid(node) != pid]
        ancestor_processes.update(new_processes)
        if len(ancestor_processes) >= threshold:
            return True
        if any((pid in proposed_parents_processes) for pid in new_processes):
            return False
        W = dag.self_predecessor(pid, dag.parents(W))
    return True



def forking_processes_in_lower_cone(dag, node):
    cone = dag.nodes_below(node)
    forkers = []
    for process_id in range(dag.n_processes):
        cone_restricted_to_process_id = [node for node in cone if dag.pid(node) == process_id]
        maximal_per_process = dag.compute_maximal_from_subset(cone_restricted_to_process_id)
        if len(maximal_per_process) > 1:
            forkers.append(process_id)
    return forkers



def check_forker_muting(dag, parents):
    all_forkers_with_evidence = []
    for U in parents:
        all_forkers_with_evidence.extend(forking_processes_in_lower_cone(dag, U))
    all_forkers_with_evidence = set(all_forkers_with_evidence)
    return all(dag.pid(U) not in all_forkers_with_evidence for U in parents)



def check_distinct_parent_processes(dag, parents):
    return len(parents) == len(set(dag.pid(parent) for parent in parents))



def check_growth(dag, node_self_predecessor, node_parents):
    assert node_self_predecessor is not None

    for parent in node_parents:
        if parent != node_self_predecessor and dag.is_reachable(parent, node_self_predecessor):
            return False
    return True



def check_introduce_new_fork(dag, pid, self_predecessor):
    assert self_predecessor is not None
    return self_predecessor not in dag.maximal_units_per_process(pid)



def check_new_unit_correctness(dag, new_unit_pid, new_unit_parents, forkers):
    '''
    Check whether the new unit does not introduce a diamond structure and
    whether the growth rule is preserved
    Returns the self_predecessor of new_unit if adding new_unit is correct and False otherwise
    '''

    self_predecessor = dag.self_predecessor(new_unit_pid, new_unit_parents)

    if self_predecessor is None:
        return False

    if new_unit_pid not in forkers and check_introduce_new_fork(dag, new_unit_pid, self_predecessor):
        return False

    if not check_growth(dag, self_predecessor, new_unit_parents):
        return False

    return self_predecessor


#======================================================================================================================


def generate_random_nonforking(n_processes, n_units, file_name = None):
    '''
    Generate a random non-forking poset with n_processes processes and optionally save it to file_name.
    :param int n_processes: the number of processes in poset
    :param int n_units: the number of units in the process beyond n_processes initial units,
    hence the total number of units is (n_processes + n_units)
    :return: a DAG instance
    '''
    process_heights = [0] * n_processes
    dag = DAG(n_processes)
    for process_id in range(n_processes):
        dag.add(generate_unit_name(0, process_id), process_id, [])

    for _ in range(n_units):
        process_id = random.sample(range(n_processes), 1)[0]
        all_but_process_id = [i for i in range(n_processes) if i != process_id]
        parent_processes = [process_id] + random.sample(all_but_process_id , 1)
        unit_height = process_heights[process_id] + 1
        unit_name = generate_unit_name(unit_height, process_id)
        dag.add(unit_name, process_id, [generate_unit_name(process_heights[i], i) for i in parent_processes])
        process_heights[process_id] += 1

    if file_name:
        dag_to_file(dag, file_name)
    return dag



def generate_random_forking(n_processes, n_units, n_forkers, file_name = None):
    '''
    Generates a random poset with n_processes processes, of which n_forkers are forking and saves it to file_name.
    The growth property is guaranteed to be satisfied and there are no "diamonds" within forking processes.
    In other words, the forking processes can only create trees.
    :param int n_processes: the number of processes in poset
    :param int n_forkers: the number of forking processes
    :param int n_units: the number of units in the process beyond genesis + n_processes initial units,
    hence the total number of units is (1 + n_processes + n_units)
    :return: a DAG instance
    '''
    forkers = random.sample(range(n_processes), n_forkers)
    node_heights = {}
    dag = DAG(n_processes)

    for process_id in range(n_processes):
        unit_name = generate_unit_name(0, process_id)
        dag.add(unit_name, process_id, [])
        node_heights[unit_name] = 0

    while len(dag) < n_processes + n_units:
        process_id = random.sample(range(n_processes), 1)[0]
        new_unit_parents = random.sample(dag.nodes.keys(), 2)
        self_predecessor = check_new_unit_correctness(dag, process_id, new_unit_parents, forkers)
        if not self_predecessor:
            continue
        new_unit_height = node_heights[self_predecessor] + 1
        new_unit_no = count_nodes_by_process_height(dag, node_heights, process_id, new_unit_height)
        unit_name = generate_unit_name(new_unit_height, process_id, new_unit_no)
        dag.add(unit_name, process_id, new_unit_parents)
        node_heights[unit_name] = new_unit_height

    if file_name:
        dag_to_file(dag, file_name)

    return dag



def generate_random_violation(n_processes, n_correct_units, n_forkers, ensure, violate):
    forkers = random.sample(range(n_processes), n_forkers)
    node_heights = {}
    dag = DAG(n_processes)
    topological_list = []

    for process_id in range(n_processes):
        unit_name = generate_unit_name(0, process_id)
        dag.add(unit_name, process_id, [])
        node_heights[unit_name] = 0
        topological_list.append(unit_name)

    it = 0
    terminate_poset = False
    while not terminate_poset:
        it += 1
        assert it < 1000*(n_processes + n_correct_units), "The random process had troubles to terminate."
        assert len(dag) < 100*(n_processes + n_correct_units), "The random process had troubles to terminate."

        process_id = random.sample(range(n_processes), 1)[0]
        new_unit_parents = random.sample(dag.nodes.keys(), 2)
        self_predecessor = dag.self_predecessor(process_id, new_unit_parents)
        if self_predecessor is None:
            continue
        if process_id not in forkers and check_introduce_new_fork(dag, process_id, self_predecessor):
            continue

        property_table = {}
        property_table['growth'] = check_growth(dag, self_predecessor, new_unit_parents)
        property_table['parent_diversity'] = check_parent_diversity(dag, process_id, new_unit_parents, (n_processes + 2)//3)
        property_table['forker_muting'] = check_forker_muting(dag, new_unit_parents)
        property_table['distinct_parents'] = check_distinct_parent_processes(dag, new_unit_parents)

        if len(dag) >= n_processes + n_correct_units and constraints_satisfied(violate, property_table):
            terminate_poset = True
        elif constraints_satisfied(ensure, property_table):
            pass
        else:
            #cannot add this node to graph
            continue

        new_unit_height = node_heights[self_predecessor] + 1
        new_unit_no = count_nodes_by_process_height(dag, node_heights, process_id, new_unit_height)
        unit_name = generate_unit_name(new_unit_height, process_id, new_unit_no)
        dag.add(unit_name, process_id, new_unit_parents)
        node_heights[unit_name] = new_unit_height
        topological_list.append(unit_name)


    return dag, topological_list


#======================================================================================================================


def generate_unit_name(unit_height, process_id, parallel_no = 0):
    if parallel_no == 0:
        name = "%d,%d" % (unit_height, process_id)
    else:
        name = "%d,%d,%d" % (unit_height, process_id, parallel_no)
    return name


def count_nodes_by_process_height(dag, node_heights, process_id, height):
    return len([node for node in node_heights if (dag.pid(node) == process_id and height == node_heights[node])])


def constraints_satisfied(constraints, truth):
    return all(truth[i] == constraints[i] for i in constraints)










