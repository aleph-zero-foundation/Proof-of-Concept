import random


def check_parent_diversity(dag, pid, parents, threshold):
    proposed_parents_processes = [dag.pid(node) for node in parents if dag.pid(node) != pid]
    ancestor_processes = set()
    W = dag.self_predecessor(pid, parents)
    while W is not None:
        new_processes = [dag.pid(node) for node in dag.parents(W) if dag.pid(node) != pid]
        ancestor_processes.update(new_processes)
        if len(ancestor_processes) >= treshold:
            return True
        if any((pid in proposed_parents_processes) for pid in new_processes):
            return False
        W = dag.self_predecessor(pid, dag.parents(W))
    return True


def check_anti_forking(dag, n_processes, U, U_parents):
    return False


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
        parent_nodes = [(generate_unit_name(process_heights[i], i), i) for i in parent_processes]
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

    it = 0
    while True:
        it += 1
        assert it < 1000*(n_processes + n_correct_units), "The random process had troubles to terminate."
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


def generate_unit_name(unit_height, process_id, parallel_no = 0):
    if parallel_no == 0:
        name = "%d,%d" % (unit_height, process_id)
    else:
        name = "%d,%d,%d" % (unit_height, process_id, parallel_no)
    return name


def count_nodes_by_process_height(node_heights, process_id, height):
    count = 0
    for node, node_height in node_heights.items():
        if node[1] == process_id and height == node_height:
            count += 1
    return count


def constraints_satisfied(constraints, truth):
    for constraint, value in constraints.items():
        if truth[constraint] != value:
            return False
    return True










