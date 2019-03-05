from aleph.utils.generic_test import simulate_with_checks
import aleph.const as consts
import random


def toss_for_prime(U, poset, dag, results, additional_args):
    '''
    For every prime unit of level>=5 try to toss a coin for some random prime at level>=3 that is >=4 levels below.
    '''
    if additional_args is None:
        primes = []
    else:
        primes = additional_args
    if poset.is_prime(U) and U.level>=3:
        primes.append(U)
        if U.level>=7:
            random.shuffle(primes)
            # find any prime unit that is >=4 levels below U
            for U_c in primes:
                if U_c.level<=U.level - 4:
                    results.append((U.level, poset.toss_coin(U_c, U)))
                    break
    return primes


def test_threshold_coin_toss():
    '''
    Test whether the toss_coin code succeeds (i.e. really whether it terminates with no exception).
    '''
    n_processes = 6
    n_units = 320
    results = simulate_with_checks(
            n_processes,
            n_units,
            n_forkers = 0,
            post_prepare = toss_for_prime,
            use_tcoin = True,
            seed = 0)
    # the poset should be high enough so that toss_for_prime produces some coin tosses
    assert len(results) > 0
    # we should reach beyond const.ADD_SHARES so that at least one coin toss happens by combining shares and not using simple_coin
    assert results[-1][0] > consts.ADD_SHARES, "Too low poset generated"


