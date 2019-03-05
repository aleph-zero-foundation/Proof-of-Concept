from aleph.utils.dag_utils import generate_random_forking, generate_random_nonforking
from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.crypto.crp import CommonRandomPermutation
from aleph.data_structures import Unit, Poset
from aleph.crypto.signatures.threshold_signatures import generate_keys
from aleph.crypto.threshold_coin import ThresholdCoin
from aleph.utils import dag_utils
from aleph.utils.dag import DAG
from aleph.actions import create_unit
import random

def generate_and_check_dag(checks, n_processes, n_units, forking = None, repetitions = 1, seed = 123456789):
    '''
    Generate a dag with given number of processes, forking processes and units and pull it through a list of correctness checks.
    :param list checks: a list of functions that take a DAG as input and run some correctness check/s (using asserts)
    :param int n_processes: the number of processes in the dag
    :param int n_units: the number of units that should be created
    :param function forking: a function that outputs the number of forkers we want in the poset
                             Note: this is a function since we sometimes want it to be generated randomly after setting the seed
    :param int repetitions: how many times to repeat this procedure
    :param int seed: a seed for generating randomness by the "random" module
    '''
    random.seed(seed)
    for _ in range(repetitions):
        if forking is not None:
            dag = generate_random_forking(n_processes, n_units, forking())
        else:
            dag = generate_random_nonforking(n_processes, n_units)
        for check in checks:
            check(dag)


def generate_unit(dag, posets, creator_id, unit_to_name, forking, only_maximal_parents):
    '''
    Create a new unit
    :param DAG dag:
    :param list posets:
    :param int creator_id:
    :param dict unit_to_name:
    :param bool forking:
    :param bool only_maximal_parents:
    '''
    n_processes = len(posets)
    if forking:
        return dag_utils.generate_random_compliant_unit(dag, n_processes, creator_id, forking = True, only_maximal_parents=only_maximal_parents)
    U = create_unit(posets[creator_id], creator_id, [])
    if U is None:
        return None
    dag_parents = [unit_to_name[creator_id][V] for V in U.parents]
    dag_name = dag_utils.generate_unused_name(dag, creator_id)
    return U, dag_name, dag_parents

def generate_crp(n_processes):
    signing_keys = [SigningKey() for _ in range(n_processes)]
    public_keys = [VerifyKey.from_SigningKey(sk) for sk in signing_keys]
    return CommonRandomPermutation([pk.to_hex() for pk in public_keys])

def initialize_posets(n_processes, use_tcoin = False):
    crp = generate_crp(n_processes)
    return [Poset(n_processes, process_id, crp, use_tcoin = use_tcoin) for process_id in range(n_processes)]

def distribute_unit(U, name, posets, forkers, name_to_unit, unit_to_name):
    for process_id in range(len(posets)):
        if process_id == U.creator_id or process_id in forkers:
            continue
        parent_hashes = [V.hash() for V in U.parents]
        parents = [posets[process_id].units[V] for V in parent_hashes]
        U_new = Unit(U.creator_id, parents, U.transactions(), U.signature, U.coin_shares)
        posets[process_id].prepare_unit(U_new)
        assert posets[process_id].check_compliance(U_new), f'{U_new.creator_id} {U_new.level}'
        posets[process_id].add_unit(U_new)
        name_to_unit[process_id][name] = U_new
        unit_to_name[process_id][U_new] = name

def verify_nonforker_fails(dag, n_processes, creator_id, forkers, only_maximal_parents):
    if creator_id not in forkers:
        res = dag_utils.generate_random_compliant_unit(dag, n_processes, creator_id, forking = False, only_maximal_parents=only_maximal_parents)
        assert res is None, f"Impossibly generated {res[0]}, with parents {res[1]}"

def simulate_with_checks(
        n_processes,
        n_units,
        n_forkers,
        verify_fails = verify_nonforker_fails,
        post_prepare = lambda *args: None,
        only_maximal_parents = True,
        use_tcoin = False,
        seed = 1729):
    random.seed(seed)
    forkers = random.sample(range(n_processes), n_forkers)
    dag = DAG(n_processes)
    posets = initialize_posets(n_processes, use_tcoin)

    name_to_unit = [{} for process_id in range(n_processes)]
    unit_to_name = [{} for process_id in range(n_processes)]

    iter_count = 0
    additional_args = None
    results = []
    while len(dag) < n_units:
        iter_count += 1
        assert iter_count < n_units*5000, "Creating %d units seems to be taking too long." % n_units
        creator_id = random.choice(range(n_processes))
        res = generate_unit(dag, posets, creator_id, unit_to_name, forking = creator_id in forkers, only_maximal_parents = only_maximal_parents)
        if res is None:
            verify_fails(dag, n_processes, creator_id, forkers, only_maximal_parents)
            continue
        U, name, parents = res
        if creator_id in forkers:
            print(U, name, parents)

        dag.add(name, creator_id, parents)

        posets[creator_id].prepare_unit(U)
        additional_args = post_prepare(U, posets[creator_id], dag, results, additional_args)
        assert posets[creator_id].check_compliance(U)
        posets[creator_id].add_unit(U)
        name_to_unit[creator_id][name] = U
        unit_to_name[creator_id][U] = name
        # "send" it to other processes
        distribute_unit(U, name, posets, forkers, name_to_unit, unit_to_name)
    return results
