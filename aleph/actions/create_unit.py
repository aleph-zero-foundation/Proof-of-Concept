'''This module implements unit creation.'''

import random
import logging

from aleph.data_structures.unit import Unit
import aleph.const as consts

def _create_dealing_unit(poset, creator_id, txs):
    U = Unit(creator_id, [], txs)
    if poset.use_tcoin:
        poset.add_tcoin_to_dealing_unit(U)
    return U

def _nonforking_creator(poset, V):
    return len(poset.max_units_per_process[V.creator_id]) == 1

def _parent_candidates(poset, parents, level):
    return list(reversed([V for V in poset.max_units if V.level == level and V not in parents and _nonforking_creator(poset, V)]))

def _combine_parents(parents, new_parents):
    if not new_parents:
        return parents
    level = new_parents[0].level
    lower_parents = [V for V in parents if V.level <= level]
    higher_parents = [V for V in parents if V.level > level]
    return lower_parents + new_parents + higher_parents

def _pick_more_parents(poset, parents, level, num_parents):
    parent_candidates = _parent_candidates(poset, parents, level)
    non_visible_primes = poset.get_all_prime_units_by_level(level)
    for V in parents:
        non_visible_primes = [W for W in non_visible_primes if not poset.below(W, V)]
    new_parents = []
    for V in parent_candidates:
        if len(new_parents) + len(parents) == num_parents:
            return _combine_parents(parents, new_parents)
        # the 3 lines below can be optimized (by a constant factor) but it is left as it is for simplicity
        if any(poset.below(W, V) for W in non_visible_primes):
            new_parents.append(V)
            non_visible_primes = [W for W in non_visible_primes if not poset.below(W, V)]
    return _combine_parents(parents, new_parents)

def create_unit(poset, creator_id, txs, num_parents = None):
    '''
    Creates a new unit and stores txs in it. It uses only maximal units in the poset as parents (with the possible exception for the self_predecessor).
    This parent selection strategy has the following properties:
    - the created unit satisfies the expand-primes rule,
    - the main objective is for the new unit to see as many units as possible, but at the same time to not have too many parents
    NOTE: this strategy is most effective when num_parents is quite high, ideally unrestricted.

    :param Poset poset: poset in which the new unit is created
    :param int creator_id: id of process creating the new unit
    :param list txs: list of correct transactions
    :param int num_parents: maximum number of distinct parents (lower bound is always 2)
    :returns: the new-created unit, or None if it is not possible to create a compliant unit using this strategy
    '''
    num_parents = consts.N_PARENTS if num_parents is None else num_parents
    logger = logging.getLogger(consts.LOGGER_NAME)
    logger.info(f"create: {creator_id} attempting to create a unit.")

    if not poset.max_units_per_process[creator_id]:
        U = _create_dealing_unit(poset, creator_id, txs)
        logger.info(f"create: {creator_id} created its dealing unit.")
        return U

    assert len(poset.max_units_per_process[creator_id]) == 1, "It appears we have created a fork."

    # choose parents for the new unit
    U_self_predecessor = poset.max_units_per_process[creator_id][0]
    level_predecessor = U_self_predecessor.level
    level_poset = poset.level_reached
    parents = [U_self_predecessor]
    # we start by picking parents of level = level_poset
    parents = _pick_more_parents(poset, parents, level_poset, num_parents)

    if level_poset > level_predecessor and len(parents) < num_parents:
        # We expect here that level_poset = level_predecessor + 1.
        # We will add a bunch of parents of level = level_predecessor, the reason to do that is to make the new unit "see" as many
        # units as possible. This in turn is to make the poset "better connected" which helps in fast proofs of popularity for timing units.
        parents = _pick_more_parents(poset, parents, level_predecessor, num_parents)

    if len(parents) < 2:
        return None

    U = Unit(creator_id, parents, txs)

    if poset.use_tcoin:
        # we need to call prepare_unit to fill some fields necessary for determining which coin shares to add
        poset.prepare_unit(U)
        if poset.is_prime(U) and U.level >= consts.ADD_SHARES:
            poset.add_coin_shares(U)

    return U
