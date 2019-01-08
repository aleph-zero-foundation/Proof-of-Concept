import random
from itertools import cycle

from aleph.data_structures import Unit, Poset, UserDB
from aleph.process import Process
from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.utils.dag_utils import generate_random_compliant_unit
from aleph.utils import DAG, dag_utils
from aleph.utils.testing_utils import add_to_instance
from aleph.utils.plot import plot_poset, plot_dag





def test_delta_level_4_no_decision():
    random.seed(123456789)
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




def add_new_unit_with_given_parents(process, processes, dag, names_to_units, maximal_names, creator_id, parent_ids_set):
    '''
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
            if dag_utils.check_parent_diversity(dag, creator_id, parent_names, (n_processes+2)//3):
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
    Start with a set of processes processes_to_advance with max-units at level next_level.
    Keep adding new units within this set (and having parents within this set) until all of them reach level next_level.
    Note that it is necessary that |processes_to_advance|>=
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



def unit_to_name(names_to_units, U):
    for (name, V) in names_to_units.items():
        if V is U:
            return name
    assert False, "Unit not found in the dictionary."
    return None







def generate_delta_level_4_no_decision(det_coin_value = 0):

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
        new_process = Process(n_processes, process_id, sk, pk, addresses, public_keys, recv_addresses[process_id], None, 'LINEAR_ORDERING')
        processes.append(new_process)

    process = processes[0]

    # make the permutation deterministic to control the order in which units are considered as candidates for timing units
    process.poset.crp = DeterministicPermutation(range(n_processes))

    @add_to_instance(process.poset)
    def toss_coin(self, U_c, tossing_unit):
        return det_coin_value


    # relaxing the growth and parent_diversity rules so that it is not that hard to construct this example
    @add_to_instance(process.poset)
    def check_growth(self, U):
        return True

    @add_to_instance(process.poset)
    def check_parent_diversity(self, U):
        return True


    dag = DAG(n_processes)
    names_to_units = {}
    all_ids = list(range(n_processes))
    left1 = [0]
    right3 = list(range(1,n_processes))

    maximal_names = [None for _ in range(n_processes)]

    # create dealing units for every process
    for process_id in range(n_processes):
        add_new_unit_with_given_parents(process, processes, dag, names_to_units, maximal_names, process_id, [])


    # reach level 1 with every process
    for next_level in [1,2]:
        reach_new_level_with_processes(process, processes, dag, names_to_units, maximal_names, right3, next_level)
        add_new_unit_with_given_parents(process, processes, dag, names_to_units, maximal_names, 0, right3)

        #verify whether all processes reached the required level
        for process_id in range(n_processes):
            name_maximal = maximal_names[process_id]
            assert names_to_units[name_maximal].level == next_level


    # reach level 1 with every process
    order_primes = reach_new_level_with_processes(process, processes, dag, names_to_units, maximal_names, right3, 3)
    low = order_primes[0]
    middle = order_primes[1]
    high = order_primes[2]
    add_new_unit_with_given_parents(process, processes, dag, names_to_units, maximal_names, low, [0])
    add_new_unit_with_given_parents(process, processes, dag, names_to_units, maximal_names, middle, [low])
    if not add_new_unit_with_given_parents(process, processes, dag, names_to_units, maximal_names, 0, [middle]):
        return False

    #verify whether all processes reached the required level
    for process_id in range(n_processes):
        name_maximal = maximal_names[process_id]
        assert names_to_units[name_maximal].level == 3





    undecided = [0, low, middle]

    #plot_dag(dag)


    reach_new_level_with_processes(process, processes, dag, names_to_units, maximal_names, undecided, 4)

    add_new_unit_with_given_parents(process, processes, dag, names_to_units, maximal_names, high, undecided)

    # it is now guaranteed that delta at level 4 is \bottom


    #plot_dag(dag)





    for next_level in [5,6,7]:

        reach_new_level_with_processes(process, processes, dag, names_to_units, maximal_names, all_ids, next_level)

        for process_id in all_ids:
            name_maximal = maximal_names[process_id]
            print(names_to_units[name_maximal].level, next_level)



    poset = process.poset
    prime_units_level_process = [[None for _ in range(n_processes)]]
    for level in range(1,8):
        prime_units_level_process.append([poset.get_prime_units_by_level_per_process(level)[process_id][0] for process_id in range(n_processes)])
    # the prime unit by process 0 at level 1 is not chosen as timing
    #assert poset.timing_units[0] is prime_units_level_process[1][1]
    #assert poset.timing_units[0] is prime_units_level_process[1][0]
    U_c = prime_units_level_process[1][0]
    poset.timing_partial_results[U_c.hash()] = {}


    for level in [2,3,4,5,6,7]:
        pi_vals = [poset.compute_pi(U_c, U) for U in prime_units_level_process[level]]
        print(level, pi_vals)
        if level in [2,3]:
            assert pi_vals == [1,0,0,0]
        if level == 4:
            assert pi_vals == [-1,-1,-1,-1]
        if level in [5,6,7]:
            assert pi_vals == [det_coin_value]*n_processes


    for level in [3,5,7]:
        delta_vals = [poset.compute_delta(U_c, U) for U in prime_units_level_process[level]]
        if level == 3:
            assert delta_vals == [0,0,0,0]
        if level == 5:
            assert delta_vals == [-1,-1,-1,-1]
        if level == 7:
            assert det_coin_value in delta_vals and (1-det_coin_value) not in delta_vals
        #print(level, delta_vals)

    if det_coin_value == 0:
        assert poset.timing_units[0] is prime_units_level_process[1][1]
    if det_coin_value == 1:
        assert poset.timing_units[0] is prime_units_level_process[1][0]
    #print(unit_to_name(names_to_units, poset.timing_units[0]))
    #print(unit_to_name(names_to_units, prime_units_level_process[1][0]))
    #plot_dag(dag)


    return True


    '''



    for unit_no in range(n_units):
        while True:
            creator_id = random.choice(range(n_processes))
            gen_unit = generate_random_compliant_unit(dag, n_processes, process_id = creator_id, forking = False, only_maximal_parents = True)
            if gen_unit is not None:
                name, parent_names = gen_unit
                break
        #print(name, parent_names)
        #print(creator_id)
        parents = [names_to_units[par_name] for par_name in parent_names]
        U = Unit(creator_id, parents, txs=[])
        processes[creator_id].sign_unit(U)
        names_to_units[name] = U
        process.poset.prepare_unit(U)
        if not process.add_unit_to_poset(U):
            print(f'Unit {name} not compliant.')
            exit(0)
        dag.add(name, creator_id, parent_names)
        if unit_no%100 == 0:
            print(f"Adding unit no {unit_no + n_processes} out of {n_units + n_processes}.")

    '''



test_delta_level_4_no_decision()