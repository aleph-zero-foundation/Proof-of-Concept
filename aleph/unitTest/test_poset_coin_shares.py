from aleph.data_structures import Unit, Poset
from aleph.crypto.crp import CommonRandomPermutation
from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.crypto.signatures.threshold_signatures import generate_keys
from aleph.crypto.threshold_coin import ThresholdCoin
from aleph.utils import dag_utils
from aleph.utils.dag import DAG
import random


def check_create_unit(n_processes, n_units, n_forkers, strategy, verify_fails = False):
    forkers = random.sample(range(n_processes), n_forkers)
    non_forkers = [process_id for process_id in range(n_processes) if process_id not in forkers]
    dag = DAG(n_processes)
    signing_keys = [SigningKey() for _ in range(n_processes)]
    public_keys = [VerifyKey.from_SigningKey(sk) for sk in signing_keys]
    crp = CommonRandomPermutation([pk.to_hex() for pk in public_keys])

    posets = [Poset(n_processes, crp) for process_id in range(n_processes)]
    # let every process generate threshold coins for all processes
    for dealer_id in range(n_processes):
        vk, sks = generate_keys(n_processes, n_processes//3)
        for process_id in range(n_processes):
            threshold_coin = ThresholdCoin(dealer_id, process_id, n_processes, n_processes//3, sks[process_id], vk)
            posets[process_id].add_threshold_coin(threshold_coin)

    name_to_unit = [{} for process_id in range(n_processes)]
    unit_to_name = [{} for process_id in range(n_processes)]

    iter_count = 0
    levels = list(range(100))
    while len(dag) < n_units:
        iter_count += 1
        assert iter_count < n_units*5000, "Creating %d units seems to be taking too long." % n_units
        creator_id = random.choice(range(n_processes))
        if creator_id in forkers:
            res = dag_utils.generate_random_compliant_unit(dag, n_processes, creator_id, forking=True, only_maximal_parents=True)
            if res is None:
                continue
            name, parents = res
        else:
            U = posets[creator_id].create_unit(creator_id, [], strategy = strategy, num_parents = 2)
            if U is None:
                if verify_fails:
                    res = dag_utils.generate_random_compliant_unit(dag, n_processes, creator_id, forking=False, only_maximal_parents=True)
                    assert res is None
                continue
            parents = [unit_to_name[creator_id][V] for V in U.parents]
            name = dag_utils.generate_unused_name(dag, creator_id)

        dag.add(name, creator_id, parents)

        # create unit
        parent_units = [name_to_unit[creator_id][parent_name] for parent_name in parents]

        U = Unit(creator_id = creator_id, parents = parent_units, txs = [])
        posets[creator_id].prepare_unit(U, True)
        if U.level in levels:
            levels.remove(U.level)
            print(U.level, len(dag))
        assert posets[creator_id].check_compliance(U)
        posets[creator_id].add_unit(U)
        name_to_unit[creator_id][name] = U
        unit_to_name[creator_id][U] = name
        # "send" it to other processes
        for process_id in non_forkers:
            if process_id == creator_id:
                continue
            posets[creator_id].prepare_unit(U)
            assert posets[process_id].check_compliance(U), f'{U.creator_id} {U.level}'
            posets[process_id].add_unit(U)
            name_to_unit[process_id][name] = U
            unit_to_name[process_id][U] = name

def run(repetitions, min_proc, max_proc, units_per_process):
    random.seed(123456789)
    for strategy in ["link_self_predecessor", "link_above_self_predecessor"]:
        for rep in range(repetitions):
            n_processes = random.randint(min_proc, max_proc)
            n_units = random.randint(0, units_per_process)
            n_forkers = random.randint(0,n_processes//3)
            print('test setting', rep, n_processes, n_units, n_forkers)
            print('level, size')
            check_create_unit(n_processes, n_units, n_forkers, strategy, verify_fails = True)


def test_level():
    run(10, 6, 6, 300)
