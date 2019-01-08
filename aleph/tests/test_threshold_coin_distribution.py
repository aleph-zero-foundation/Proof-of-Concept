from aleph.data_structures import Unit, Poset
from aleph.crypto.crp import CommonRandomPermutation
from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.crypto.signatures.threshold_signatures import generate_keys
from aleph.crypto.threshold_coin import ThresholdCoin
from aleph.utils import dag_utils
from aleph.utils.dag import DAG
import random
from joblib import Parallel, delayed
from time import time


def check_create_unit(n_processes, n_units, n_forkers, strategy, verify_fails = False, seed=1729):
    random.seed(seed)
    forkers = random.sample(range(n_processes), n_forkers)
    non_forkers = [process_id for process_id in range(n_processes) if process_id not in forkers]
    dag = DAG(n_processes)
    signing_keys = [SigningKey() for _ in range(n_processes)]
    public_keys = [VerifyKey.from_SigningKey(sk) for sk in signing_keys]
    crp = CommonRandomPermutation([pk.to_hex() for pk in public_keys])

    posets = [Poset(n_processes, crp) for process_id in range(n_processes)]
    # let every process generate threshold coins for all processes
    for dealer_id in range(n_processes):
        vk, sks = generate_keys(n_processes, n_processes//3+1)
        for process_id in range(n_processes):
            threshold_coin = ThresholdCoin(dealer_id, process_id, n_processes, n_processes//3+1, sks[process_id], vk)
            posets[process_id].add_threshold_coin(threshold_coin)

    name_to_unit = [{} for process_id in range(n_processes)]
    unit_to_name = [{} for process_id in range(n_processes)]

    iter_count = 0
    levels = list(range(1000))
    U_c = None
    results = []
    while len(dag) < n_units:
        iter_count += 1
        assert iter_count < n_units*5000, "Creating %d units seems to be taking too long." % n_units
        creator_id = random.choice(range(n_processes))
        if creator_id in forkers:
            res = dag_utils.generate_random_compliant_unit(dag, n_processes, creator_id, forking = True, only_maximal_parents=True)
            if res is None:
                continue
            name, parents = res
        else:
            U = posets[creator_id].create_unit(creator_id, [], strategy = strategy, num_parents = 2)
            if U is None:
                if verify_fails:
                    res = dag_utils.generate_random_compliant_unit(dag, n_processes, creator_id, forking = False, only_maximal_parents=True)
                    assert res is None
                continue
            parents = [unit_to_name[creator_id][V] for V in U.parents]
            name = dag_utils.generate_unused_name(dag, creator_id)

        dag.add(name, creator_id, parents)

        # create unit
        parent_units = [name_to_unit[creator_id][parent_name] for parent_name in parents]

        U = Unit(creator_id = creator_id, parents = parent_units, txs = [])
        posets[creator_id].prepare_unit(U, True)
        # below condition will triger exactly once for each level
        if U.level in levels:
            levels.remove(U.level)
            print(U.level, len(dag))
            if U.level == 3:
                U_c = U
            if U_c is not None and U.level-U_c.level >= 4:
                # print(f'tossing!!! U_c: creator_id {U_c.creator_id} U: creator_id={U.creator_id} level={U.level}    result: {posets[creator_id].toss_coin(U_c, U)}')
                results.append(posets[creator_id].toss_coin(U_c, U))
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
    return results

def check_distribution(n_jobs, n_processes, n_units, n_forkers, strategy, verify_fails = True):
    print(n_jobs, n_processes, n_units, n_forkers, strategy, verify_fails)
    print('dispatching workers')
    start = time()
    results = Parallel(n_jobs=n_jobs)(delayed(check_create_unit)(n_processes, n_units, n_forkers, strategy, verify_fails, round(time())+i) for i in range(n_jobs))
    print('work done in', round(time()-start,2))

    all_sum, all_len = 0, 0
    for res in results:
        all_sum += sum(res)
        all_len += len(res)
    if all_len:
        print(all_len, all_sum/all_len)


if __name__ == '__main__':
    check_distribution(8, 16, 4000, 0, 'link_self_predecessor')
