import random
from itertools import combinations
from .dag import DAG
from aleph.data_structures import Poset, Unit



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
        process_id = random.choice(range(n_processes))
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
        process_id = random.choice(range(n_processes))
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


def generate_random_compliant_unit(dag, n_processes, process_id = None, forking = False):
    '''
    Generates a random compliant unit created by a given process_id (or random process).
    '''
    if process_id is None:
        process_id = random.choice(range(n_processes))

    unit_pairs = list(combinations(dag.nodes.keys(), 2))
    random.shuffle(unit_pairs)

    for U1, U2 in unit_pairs:
        new_unit_parents = [U1, U2]
        self_predecessor = dag.self_predecessor(process_id, new_unit_parents)
        if self_predecessor is None:
            continue

        if not forking and check_introduce_new_fork(dag, process_id, self_predecessor):
            continue

        if not check_growth(dag, self_predecessor, new_unit_parents):
            continue

        if not check_parent_diversity(dag, process_id, new_unit_parents, (n_processes + 2)//3):
            continue

        if not check_forker_muting(dag, new_unit_parents):
            continue

        if not check_distinct_parent_processes(dag, new_unit_parents):
            continue

        random.shuffle(new_unit_parents)
        return generate_unused_name(dag, process_id), new_unit_parents

    return None



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

        process_id = random.choice(range(n_processes))
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

def generate_unused_name(dag, process_id):
    '''
    Generates a random string name for a node, that does yet exist in dag.
    '''
    name = ""
    name_len = 0
    while str(process_id)+"-"+name in dag:
        name_len += 1
        name = ''.join(random.sample('ABCDEFGHIJKLMNOPQRSTUVWXYZ', name_len))

    return str(process_id)+"-"+name



def count_nodes_by_process_height(dag, node_heights, process_id, height):
    return len([node for node in node_heights if (dag.pid(node) == process_id and height == node_heights[node])])


def constraints_satisfied(constraints, truth):
    return all(truth[i] == constraints[i] for i in constraints)



#======================================================================================================================
#======================================================================================================================
#======================================================================================================================



def poset_from_dag(dag, process_id=0, secret_key=None, public_key=None):
    poset = Poset(n_processes=dag.n_processes, process_id=process_id, secret_key=secret_key, public_key=public_key)
    unit_dict = {}

    for unit_name in dag.sorted():
        creator_id = dag.pid(unit_name)
        assert 0 <= creator_id <= dag.n_processes - 1, "Incorrect process id"

        assert unit_name not in unit_dict, "Duplicate unit name %s" % unit_name
        for parent in dag.parents(unit_name):
            assert parent in unit_dict, "Parent %s of unit %s not known" % (parent, unit_name)

        U = Unit(creator_id = creator_id, parents = [unit_dict[parent] for parent in dag.parents(unit_name)],
                txs = [])
        poset.add_unit(U)
        unit_dict[unit_name] = U

    return poset, unit_dict



def create_node_line(node, process_id, parents):
    line = '%s %d' % (node, process_id)
    for parent in parents:
        line += ' ' + parent
    line += '\n'
    return line



def dag_to_file(dag, file_name):
    topological_list = dag.sorted()
    with open(file_name, 'w') as f:
        f.write('%d\n' % dag.n_processes)
        for node in topological_list:
            f.write(create_node_line(node, dag.pid(node), dag.parents(node)))


def dag_from_poset(poset):
    dag = DAG(poset.n_processes)
    unit_to_name = {}
    for _ in poset.units.items():
        for U_hash, U in poset.units.items():
            if U in unit_to_name:
                continue
            if all((unit_to_name[V] in dag) for V in U.parents):
                new_name = generate_unused_name(dag, U.creator_id)
                dag.add(new_name, U.creator_id, [unit_to_name[V] for V in U.parents])
                unit_to_name[U] = new_name

    return dag, unit_to_name



def dag_from_file(file_name):
    with open(file_name) as poset_file:
        lines = poset_file.readlines()

    n_processes = int(lines[0])
    dag = DAG(n_processes)

    for line in lines[1:]:
        tokens = line.split()
        unit_name = tokens[0]
        creator_id = int(tokens[1])
        assert 0 <= creator_id <= n_processes - 1, "Incorrect process id"
        parents = tokens[2:]
        assert unit_name not in dag, "Duplicate unit name %s" % unit_name
        for parent in parents:
            assert parent in dag, "Parent %s of a unit %s not known" % (parent, unit_name)

        dag.add(unit_name, creator_id, parents)

    return dag










