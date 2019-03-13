'''This module implements unit creation.'''

import random
import logging

from aleph.data_structures.unit import Unit
import aleph.const as consts


def recent_parents_restricted(poset, W, parent_processes):
    '''
    Compute ids of processes recently used to parent units.
    :param Poset poset: the poset in which the unit lives
    :param Unit W: the unit whose ancestors are being inspected
    :param list parent_processes: processes already chosen as parents for the new unit, just so we don't choose them again
    :returns: the set of recent parents of minimal size exceeding one third of the processes or all the parents if no such set exists
    '''

    recent_parents = set()
    threshold = (poset.n_processes+2)//3
    while len(recent_parents) < threshold and W.parents:
        parents = [V.creator_id for V in W.parents if V.creator_id != W.creator_id if V.creator_id != W.creator_id and V.creator_id not in recent_parents]

        if len(recent_parents) + len(parents) >= threshold:
            break

        recent_parents.update(parents)
        W = W.self_predecessor

    # already used parents are also restricted
    recent_parents.update(parent_processes)

    return recent_parents


def growth_restricted(poset, W, parent_processes):
    '''
    Compute ids of processes whose highest unit is below the given one.
    :param Poset poset: the poset in which the unit lives
    :param Unit W: the unit whose ancestors are being inspected
    :param list parent_processes: processes already chosen as parents for the new unit, just so we don't choose them again
    :returns: the set of parents whose top unit is below the given unit
    '''

    below_W = set(V.creator_id for Vs in poset.max_units_per_process for V in Vs
                  if poset.below(V, W) and V.creator_id != W.creator_id)

    # already used parents are also restricted
    below_W.update(parent_processes)

    return below_W


def expand_primes_restricted(poset, W, parent_processes):
    '''
    Compute ids of processes whose top units are both of level not higher than parent_processes[-1].level
    and not above more prime units of level parent_processes[-1].level than all parent_processes together.
    :param Poset poset: the poset in which the unit lives
    :param Unit W: the unit that defines the set of prime units to inspect
    :param list parent_processes: processes already chosen as parents for the new unit
    :returns: the set of ids satisfying the conditions above
    '''

    not_extending_primes = set()
    level = max(poset.max_units_per_process[pid][0].level for pid in parent_processes)
    prime_below_parents = set()
    for process_id in parent_processes:
        prime_below_parents.update(poset.get_prime_units_at_level_below_unit(level, poset.max_units_per_process[process_id][0]))

    unseen_primes = set()
    for primes in poset.get_prime_units_by_level_per_process(level):
        if all(prime not in prime_below_parents for prime in primes):
            unseen_primes.update(primes)

    for Vs in poset.max_units_per_process:
        # always allow higher level units; only checking level of Vs[0], since we pick it anyways
        if Vs and Vs[0].level > level:
            continue

        for V in Vs:
            extends_primes = False
            for W in unseen_primes:
                if poset.below(W, V):
                    extends_primes = True
                    break
            if not extends_primes:
                not_extending_primes.add(V.creator_id)

    return not_extending_primes


def parents_allowed_with_restrictions(poset, creator_id, restrictions, parent_processes):
    '''
    Compute ids of processes which are allowed to be parents for the unit being created
    :param Poset poset: the poset in which the unit lives
    :param int creator_id: the id of the process which creates the unit
    :param list restrictions: functions producing sets of forbidden parent ids
    :param list parent_processes: process ids already chosen as parents
    :returns: the set of ids of processes that are valid parents for the new unit
    '''

    assert poset.max_units_per_process[creator_id], 'Trying to find parents for dealing unit'

    U_max = poset.max_units_per_process[creator_id][0]

    single_tip_processes = set(pid for pid in range(poset.n_processes) if len(poset.max_units_per_process[pid]) == 1)

    restricted_set = set()
    for restriction in restrictions:
        restricted_set.update(restriction(poset, U_max, parent_processes))

    return list(single_tip_processes - restricted_set)


def create_unit(poset, creator_id, txs, num_parents = None, restrictions=[expand_primes_restricted], force_parents = None, prefer_maximal = False):
    '''
    Creates a new unit and stores txs in it. Correctness of the txs is checked by a thread listening for new transactions.
    :param Poset poset: poset in which the new unit is created
    :param int creator_id: id of process creating the new unit
    :param list txs: list of correct transactions
    :param int num_parents: maximum number of distinct parents (lower bound is always 2)
    :param list restrictions: functions producing sets of forbidden parent ids
    :param list force_parents: (ONLY FOR DEBUGGING/TESTING) parents (units) for the created unit
    :param bool prefer_maximal: whether when choosing parents the globally maximal units in the poset are preferred over non-maximal
    :returns: the new-created unit, or None if it is not possible to create a compliant unit
    '''

    num_parents = consts.N_PARENTS if num_parents is None else num_parents
    # NOTE: perhaps we (as an honest process) should always try (if possible)
    # NOTE: to create a unit that gives evidence of another process forking
    logger = logging.getLogger(consts.LOGGER_NAME)
    logger.info(f"create: {creator_id} attempting to create a unit.")

    if not poset.max_units_per_process[creator_id]:
        # this is going to be our dealing unit
        if force_parents is not None:
            assert force_parents == [], "A dealing unit should be created first."
        U = Unit(creator_id, [], txs)
        if poset.use_tcoin:
            poset.add_tcoin_to_dealing_unit(U)
        logger.info(f"create: {creator_id} created its dealing unit.")

        return U

    assert len(poset.max_units_per_process[creator_id]) == 1, "It appears we have created a fork."

    if force_parents is not None:
        assert len(force_parents) <= num_parents and len(force_parents) > 1, "Incorrect number of parents chosen."

    if force_parents is None:
        # choose parents for the new unit
        parent_processes = [creator_id]

        while len(parent_processes) < num_parents:
            legit_parents = parents_allowed_with_restrictions(poset, creator_id, restrictions, parent_processes)

            if not legit_parents:
                if len(parent_processes) > 1:
                    #got at least two parents, it's fine
                    break
                else:
                    return None

            if prefer_maximal:
                maximal_process_ids = set(V.creator_id for V in poset.max_units)
                legit_max_parents = [parent for parent in legit_parents if parent in maximal_process_ids]
                if legit_max_parents:
                    parent_processes.append(random.choice(legit_max_parents))
                else:
                    parent_processes.append(random.choice(legit_parents))
            else:
                parent_processes.append(random.choice(legit_parents))

        U = Unit(creator_id, [poset.max_units_per_process[pid][0] for pid in parent_processes], txs)
    else:
        # force_parents is set
        assert all(V.hash() in poset.units for V in force_parents)
        # compliance might still fail here -- but it will be detected later
        # force_parents should be used for debugging and testing purposes only
        U = Unit(creator_id, force_parents, txs)

    if poset.use_tcoin:
        # TODO: calling prepare unit here is a bit confusing, maybe we can move it somewhere
        poset.prepare_unit(U)
        if poset.is_prime(U) and U.level >= consts.ADD_SHARES:
            poset.add_coin_shares(U)

    return U


def greedly_order_max_units(poset, min_level, skip):
    '''
    Order the list of maximal units in poset in which they are then considered as parents of a new unit.
    :param Poset poset:
    :param int min_level:
    :param list skip:
    :returns: A list of maximal units in poset with level >= min_level, with units in skip skipped order according to:
              - primary order key is level (increasing)
              - secondary key is the time (from most recent to least recent) when the unit was added to the poset
    '''
    greedy_list = []
    for level in range(min_level, poset.level_reached + 1):
        greedy_list += reversed([V for V in poset.max_units if V.level == level and V not in skip])
    return greedy_list



def create_unit_greedy(poset, creator_id, txs, num_parents = None, force_parents = None):
    '''
    Creates a new unit and stores txs in it. It uses only maximal units in the poset as parents (with the possible exception for the self_predecessor).
    This parent selection strategy has the following properties:
       - the created unit satisfies the expand-primes rule,
       - the main objective is for the new unit to see as many units as possible, but at the same time to not have too many parents
       - WARNING: this version might create non-compliant units when poset has forks (it is easy to fix though)
    NOTE: this strategy is most effective when num_parents is quite high, ideally unrestricted.
    :param Poset poset: poset in which the new unit is created
    :param int creator_id: id of process creating the new unit
    :param list txs: list of correct transactions
    :param int num_parents: maximum number of distinct parents (lower bound is always 2)
    :param list force_parents: (ONLY FOR DEBUGGING/TESTING) parents (units) for the created unit
    :returns: the new-created unit, or None if it is not possible to create a compliant unit using this strategy
              NOTE: this does not rule out that the standard create_unit can create a unit since it is not restricted to use maximal units only
    '''
    num_parents = consts.N_PARENTS if num_parents is None else num_parents
    logger = logging.getLogger(consts.LOGGER_NAME)
    logger.info(f"create: {creator_id} attempting to create a unit.")

    if not poset.max_units_per_process[creator_id]:
        # this is going to be our dealing unit
        if force_parents is not None:
            assert force_parents == [], "A dealing unit should be created first."
        U = Unit(creator_id, [], txs)
        if poset.use_tcoin:
            poset.add_tcoin_to_dealing_unit(U)
        logger.info(f"create: {creator_id} created its dealing unit.")

        return U

    assert len(poset.max_units_per_process[creator_id]) == 1, "It appears we have created a fork."

    if force_parents is not None:
        assert len(force_parents) <= num_parents and len(force_parents) > 1, "Incorrect number of parents chosen."

    if force_parents is None:
        # choose parents for the new unit
        U_self_predecessor = poset.max_units_per_process[creator_id][0]
        level = U_self_predecessor.level

        parent_candidates = [U_self_predecessor] + greedly_order_max_units(poset, min_level = level, skip = [U_self_predecessor])
        parents = []
        non_visible_primes = poset.get_all_prime_units_by_level(level)

        # the below faithfully implements the expand_primes rule
        for V in parent_candidates:
            if len(parents) == num_parents:
                break
            if V.level > level:
                level = V.level
                non_visible_primes = poset.get_all_prime_units_by_level(level)
            # the 3 lines below can be optimized (by a constant factor) but it is left as it is for simplicity and to avoid premature optimization
            if any(poset.below(W, V) for W in non_visible_primes):
                parents.append(V)
                non_visible_primes = [W for W in non_visible_primes if not poset.below(W, V)]
        if len(parents) < 2:
            return None

        U = Unit(creator_id, parents, txs)
    else:
        # force_parents is set
        assert all(V.hash() in poset.units for V in force_parents)
        # compliance might still fail here -- but it will be detected later
        # force_parents should be used for debugging and testing purposes only
        U = Unit(creator_id, force_parents, txs)

    if poset.use_tcoin:
        # TODO: calling prepare unit here is a bit confusing, maybe we can move it somewhere
        poset.prepare_unit(U)
        if poset.is_prime(U) and U.level >= consts.ADD_SHARES:
            poset.add_coin_shares(U)

    return U