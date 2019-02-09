import random
from itertools import cycle

from aleph.data_structures import Unit, Poset, UserDB
from aleph.process import Process
from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.utils.dag_utils import generate_random_compliant_unit
from aleph.utils import DAG, dag_utils
from aleph.utils.mock import add_to_instance
from aleph.utils.plot import plot_poset, plot_dag


def test_delta_level_4_no_decision():
    random.seed(123456789)
    # tests with deterministic coin
    for det_coin_value in [0,1]:
        counter = 0
        while not generate_delta_level_4_no_decision(det_coin_value):
            counter += 1
            assert counter <= 1000, "Failed to generate a poset with prescribed properties."


class DeterministicPermutation:
    '''
    A class to replace common random permutation in the poset.
    It always outputs the same deterministic permutation.
    '''
    def __init__(self, permutation):
        self.permutation = permutation


    def __getitem__(self, k):
        """Always returns the identity permutation."""
        return list(self.permutation)


def add_new_unit_with_given_parents(process, processes, dag, names_to_units, maximal_names, creator_id, parent_ids_set, check_diversity = True):
    '''
    :param list maximal_names: list of names of maximal units per process
    :param list parent_ids_set: list of candidates for second parent
    Add a unit created by creator_id to the poset/dag so that its second parent is a process in the list parent_ids_set.
    This function has lots of parameters which are here to generate appropriate signatures, update all the relevant structures etc.
    :returns: True if created and added succesfully. False if some compliance rule made it impossible to create a unit.
    '''
    tries = 0
    success = False
    n_processes = process.n_processes

    if parent_ids_set == []:
        parent_names = []
        success = True
    else:
        # try all possible candidates for the second parent
        for second_parent in parent_ids_set:
            if second_parent == creator_id:
                continue
            parent_ids = [creator_id, second_parent]
            parent_names = [maximal_names[parent_id] for parent_id in parent_ids]
            if not check_diversity or dag_utils.check_parent_diversity(dag, creator_id, parent_names, (n_processes+2)//3):
                success = True
                break

    if not success:
        return False

    new_name = dag_utils.generate_unused_name(dag, creator_id)
    dag.add(new_name, creator_id, parent_names)
    maximal_names[creator_id] = new_name
    parent_units = [names_to_units[nm] for nm in parent_names]
    U = Unit(creator_id, parent_units, txs=[])
    processes[creator_id].sign_unit(U)
    names_to_units[new_name] = U
    process.poset.prepare_unit(U)
    assert process.add_unit_to_poset(U), f'Unit {new_name} not compliant.'

    return True


def reach_new_level_with_processes(process, processes, dag, names_to_units, maximal_names, processes_to_advance, next_level):
    '''
    Start with a set of processes processes_to_advance with max-units at level (next_level-1).
    Keep adding new units within this set (and having parents within this set) until all of them reach level next_level.
    Note that it is necessary that |processes_to_advance|>=(2/3)*n_processes for this to be possible.
    :returns: a list of process_id's from processes_to_advance in the order of reaching next_level
    '''
    processes_on_next_level = [process_id for process_id in processes_to_advance if names_to_units[maximal_names[process_id]].level >= next_level]
    assert processes_on_next_level == []
    while len(processes_on_next_level) < len(processes_to_advance):
        for process_id in processes_to_advance:
            name = maximal_names[process_id]
            if names_to_units[name].level < next_level:
                # choose parents in a random order
                parent_ids_set = processes_to_advance[:]
                random.shuffle(parent_ids_set)
                assert add_new_unit_with_given_parents(process, processes, dag, names_to_units, maximal_names, process_id, parent_ids_set)
                if names_to_units[maximal_names[process_id]].level == next_level:
                    processes_on_next_level.append(process_id)

    return processes_on_next_level


def generate_delta_level_4_no_decision(det_coin_value = 0):
    '''
    Builds a poset over 4 processes such that the decision about a unit U_c being a timing unit is made only at level level(U_c) + 6.
    :param Bool det_coin_value: The value of the common coin to be used instead of a random coin in the decision process.
    :returns: True/False depending on the success of generating an appropriate poset. Since randomness is involved, certain parts of this process
    might fail with positive probability.
    The final decision on the Unit of interest is determined by det_coin_value.
    '''

    #NOTE: the code of this test is rather long and might be hard to read. There code is interlaced by some comments but to understand what is going on it is
    # better to simply plot_dag(dag) from time to time.


    # do some boring initialization
    #### START HERE -- INITIALIZATION ####

    n_processes = 4

    processes = []
    host_ports = [8900+i for i in range(n_processes)]
    addresses = [('127.0.0.1', port) for port in host_ports]
    recv_addresses = [('127.0.0.1', 9100+i) for i in range(n_processes)]

    signing_keys = [SigningKey() for _ in range(n_processes)]
    public_keys = [VerifyKey.from_SigningKey(sk) for sk in signing_keys]

    for process_id in range(n_processes):
        sk = signing_keys[process_id]
        pk = public_keys[process_id]
        new_process = Process(n_processes, process_id, sk, pk, addresses, public_keys, recv_addresses[process_id],
                                validation_method='LINEAR_ORDERING', use_fast_poset = False)
        processes.append(new_process)

    process = processes[0]


    # make the permutation deterministic to control the order in which units are considered as candidates for timing units
    process.poset.crp = DeterministicPermutation(range(n_processes))

    @add_to_instance(process.poset)
    def toss_coin(self, U_c, tossing_unit):
        return det_coin_value

    @add_to_instance(process.poset)
    def check_coin_shares(self, U):
        return True

    # relaxing the growth and parent_diversity rules so that it is not that hard to construct this example
    @add_to_instance(process.poset)
    def check_growth(self, U):
        return True

    # this can be most likely commented out, but not really necessary to do so
    @add_to_instance(process.poset)
    def check_parent_diversity(self, U):
        return True

    # don't use the expand_primes rule, this example is not compatible with it
    # TODO(maybe): fix the example to work with expanding primes
    process.poset.compliance_rules = process.poset.default_compliance_rules
    process.poset.compliance_rules['expand_primes'] = False
    process.poset.compliance_rules['parent_diversity'] = True
    process.poset.compliance_rules['growth'] = True

    dag = DAG(n_processes)

    # mapping from names (in dag) to units in process.poset
    names_to_units = {}

    all_ids = list(range(n_processes))

    # maintain a list of names of maximal units (in dag) per process
    maximal_names = [None for _ in range(n_processes)]

    #### END HERE -- INITIALIZATION ####

    # create dealing units for every process
    for process_id in range(n_processes):
        add_new_unit_with_given_parents(process, processes, dag, names_to_units, maximal_names, process_id, [])


    right3 = list(range(1,n_processes))

    # reach level 2 with every process
    # process 0 here is meant to be "slow" and it creates only one unit per level
    for next_level in [1,2]:
        reach_new_level_with_processes(process, processes, dag, names_to_units, maximal_names, right3, next_level)
        add_new_unit_with_given_parents(process, processes, dag, names_to_units, maximal_names, 0, right3)

        #verify whether all processes reached the required level
        for process_id in range(n_processes):
            name_maximal = maximal_names[process_id]
            assert names_to_units[name_maximal].level == next_level


    # reach level 3 with every process except the leftmost
    order_primes = reach_new_level_with_processes(process, processes, dag, names_to_units, maximal_names, right3, 3)
    low = order_primes[0]
    middle = order_primes[1]
    high = order_primes[2]

    # the 3-4 lines below make sure that the new unit created by process 0 will be high above the unit created by 0 at the previous level
    # while at the same that unit is not high below any other prime unit at level 3
    if not add_new_unit_with_given_parents(process, processes, dag, names_to_units, maximal_names, low, [0]):
        return False
    # TODO(maybe): fix the example to work with parent diversity
    if not add_new_unit_with_given_parents(process, processes, dag, names_to_units, maximal_names, middle, [low], check_diversity=False):
        return False
    if not add_new_unit_with_given_parents(process, processes, dag, names_to_units, maximal_names, 0, [middle]):
        # this might fail due to bad luck, but should succeed with good probability
        return False

    #verify whether all processes reached the required level
    for process_id in range(n_processes):
        name_maximal = maximal_names[process_id]
        assert names_to_units[name_maximal].level == 3


    # this are the processes that we want to have the pi value equal to bottom (-1) at level 4 ( level(U_c) + 3 )
    undecided = [0, low, middle]

    #plot_dag(dag)


    reach_new_level_with_processes(process, processes, dag, names_to_units, maximal_names, undecided, 4)
    add_new_unit_with_given_parents(process, processes, dag, names_to_units, maximal_names, high, undecided)

    # it is now guaranteed that delta at level 4 is (-1) everywhere

    # grow the poset to level 7 arbitrarily
    for next_level in [5,6,7]:

        reach_new_level_with_processes(process, processes, dag, names_to_units, maximal_names, all_ids, next_level)

        for process_id in all_ids:
            name_maximal = maximal_names[process_id]
            #print(names_to_units[name_maximal].level, next_level)



    poset = process.poset
    prime_units_level_process = [[None for _ in range(n_processes)]]
    # extract all prime units at every level from the poset
    for level in range(1,8):
        prime_units_level_process.append([poset.get_prime_units_by_level_per_process(level)[process_id][0] for process_id in range(n_processes)])

    # This is our unit of interest!
    U_c = prime_units_level_process[1][0]

    # Initialize the partial results dictionary in the poset -- this is normally maintained by the Poset.attempt_timing_decision method,
    # however at this point it has been already cleaned up.
    poset.timing_partial_results[U_c.hash()] = {}


    #Put asserts on the expected outcomes of pi computations
    for level in [2,3,4,5,6,7]:
        pi_vals = [poset.compute_pi(U_c, U) for U in prime_units_level_process[level]]
        print(level, pi_vals)
        if level in [2,3]:
            assert pi_vals == [1,0,0,0]
        if level == 4:
            assert pi_vals == [-1,-1,-1,-1]
        if level in [5,6,7]:
            assert pi_vals == [det_coin_value]*n_processes

    #Put asserts on the expected outcomes of delta computations
    for level in [3,5,7]:
        delta_vals = [poset.compute_delta(U_c, U) for U in prime_units_level_process[level]]
        if level == 3:
            assert delta_vals == [0,0,0,0]
        if level == 5:
            assert delta_vals == [-1,-1,-1,-1]
        if level == 7:
            assert det_coin_value in delta_vals and (1-det_coin_value) not in delta_vals


    if det_coin_value == 0:
        assert poset.timing_units[0] is prime_units_level_process[1][1]
    if det_coin_value == 1:
        assert poset.timing_units[0] is prime_units_level_process[1][0]

    return True


def unit_to_name(names_to_units, U):
    '''
    A helper function to access the inverse mapping given by a dictionary names->units.
    Given a unit U it outputs a name such that names_to_units[name] = U.
    '''
    for (name, V) in names_to_units.items():
        if V is U:
            return name
    assert False, "Unit not found in the dictionary."
