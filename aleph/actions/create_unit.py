'''This module implements unit creation.'''

import random
import logging

from aleph.data_structures.unit import Unit
import aleph.const as consts

def recent_parents_restricted(poset, W, parent_processes):
    '''
    Compute ids of processes recently used to parent units.
    :param poset poset: the poset in which the unit lives
    :param unit W: the unit whose ancestors are being inspected
    :param list parent_processes: processes already chosen as parents for the new unit, just so we don't choose them again
    :returns: the set of recent parents of minimal size exceeding one third of the processes or all the parents if no such set exists
    '''
    recent_parents = set()
    threshold = (poset.n_processes+2)//3
    while len(recent_parents) < threshold:
        # W is our dealing unit -> STOP
        if len(W.parents) == 0:
            break
        parents = [V.creator_id for V in W.parents if V.creator_id != W.creator_id]
        if len(recent_parents.union(parents)) >= threshold:
            break
        recent_parents = recent_parents.union(parents)
        W = W.self_predecessor
    return recent_parents.union(parent_processes)

def growth_restricted(poset, W, parent_processes):
    '''
    Compute ids of processes whose highest unit is below the given one.
    :param poset poset: the poset in which the unit lives
    :param unit W: the unit whose ancestors are being inspected
    :param list parent_processes: processes already chosen as parents for the new unit, just so we don't choose them again
    :returns: the set of parents whose top unit is below the given unit
    '''
    below_W = set()
    for Vs in poset.max_units_per_process:
        for V in Vs:
            if poset.below(V, W) and V.creator_id != W.creator_id:
                below_W.add(V.creator_id)
    return below_W.union(parent_processes)

def expand_primes_restricted(poset, W, parent_processes):
    '''
    Compute ids of processes whose highest unit is only above prime units of level W.level that W is also above
    :param poset poset: the poset in which the unit lives
    :param unit W: the unit that defines the set of prime units to inspect
    :param list parent_processes: processes already chosen as parents for the new unit
    :returns: the set of ids satisfying the conditionss above
    '''
    not_extending_primes = set()
    level = W.level
    prime_below_parents = set()
    # we already saw enough prime units, cannot require more while using 'high above' for levels
    if 3*len(poset.get_prime_units_at_level_below_unit(level, W)) >= 2*poset.n_processes:
        return recent_parents_restricted(poset, W, parent_processes).union(growth_restricted(poset, W, parent_processes))
    for process_id in parent_processes:
        prime_below_parents.update(poset.get_prime_units_at_level_below_unit(level, poset.max_units_per_process[process_id][0]))
    for Vs in poset.max_units_per_process:
        for V in Vs:
            prime_below_V = set(poset.get_prime_units_at_level_below_unit(level, V))
            if prime_below_V <= prime_below_parents:
                not_extending_primes.add(V.creator_id)
    return not_extending_primes

def parents_allowed_with_restrictions(poset, creator_id, restrictions, parent_processes):
    '''
    Compute ids of processes which are allowed to be parents for the unit being created
    :param poset poset: the poset in which the unit lives
    :param int creator_id: the id of the process which creates the unit
    :param list restrictions: functions producing sets of forbidden parent ids
    :param list parent_processes: process ids already chosen as parents
    :returns: the set of ids of processes that are valid parents for the new unit
    '''
    U_max = poset.max_units_per_process[creator_id][0]

    single_tip_processes = set(pid for pid in range(poset.n_processes)
                            if len(poset.max_units_per_process[pid]) == 1)

    restricted_set = set()
    for restriction in restrictions:
        restricted_set.update(restriction(poset, U_max, parent_processes))

    return [pid for pid in single_tip_processes if not (pid in restricted_set)]

def create_unit(poset, creator_id, txs, num_parents = 2, restrictions=[expand_primes_restricted], force_parents = None):
    '''
    Creates a new unit and stores txs in it. Correctness of the txs is checked by a thread listening for new transactions.
    :param list txs: list of correct transactions
    :param int num_parents: maximum number of distinct parents (lower bound is always 2)
    :param list restrictions: functions producing sets of forbidden parent ids
    :param list force_parents: (ONLY FOR DEBUGGING/TESTING) parents (units) for the created unit
    :returns: the new-created unit, or None if it is not possible to create a compliant unit
    '''

    # NOTE: perhaps we (as an honest process) should always try (if possible)
    # NOTE: to create a unit that gives evidence of another process forking
    logger = logging.getLogger(consts.LOGGER_NAME)
    logger.info(f"create: {creator_id} attempting to create a unit.")
    if len(poset.max_units_per_process[creator_id]) == 0:
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
        parent_processes = [creator_id]

        while len(parent_processes) < num_parents:
            legit_parents = parents_allowed_with_restrictions(poset, creator_id, restrictions, parent_processes)

            if len(legit_parents) == 0:
                if len(parent_processes) > 1:
                    #got at least two parents, it's fine
                    break
                else:
                    return None

            parent_processes.append(random.choice(legit_parents))

        U = Unit(creator_id, [poset.max_units_per_process[pid][0] for pid in parent_processes], txs)
    else:
        # force_parents is set
        assert all(V.hash() in poset.units for V in force_parents)
        # compliance might still fail here -- but it will be detected later
        # force_parents should be used for debugging and testing purposes only
        U = Unit(creator_id, force_parents, txs)

    # TODO: calling prepare unit here is a bit confusing, maybe we can move it somewhere
    if poset.use_tcoin:
        poset.prepare_unit(U)
        if poset.is_prime(U) and U.level >= consts.ADD_SHARES:

            poset.add_coin_shares(U)


    return U


