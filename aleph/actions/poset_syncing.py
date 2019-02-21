'''This module implements functions useful for syncing posets.'''

import random
import logging

import aleph.const as consts

def poset_info(poset):
    '''
    A short representation of the poset state, for syncing purposes.
    :param Poset poset: the poset which state we want to receive
    :returns: A list of lists of pairs (height, hash), with the heights and hashes of maximal elements per process in the poset.
    '''
    def to_sync_repr(units):
        return [(U.height, U.hash()) for U in units]
    return [to_sync_repr(units) for units in poset.max_units_per_process]

def order_units_topologically(units_list):
    '''
    Outputs a topological order of units_list.
    More formally it outputs a list top_list such that:
        whenever U, V are in units_list and V is a parent of U then V appears before U in top_list.
    Note: this does not necessarily preserve the ordering in the poset!
    :param list unit_list: the list of units to sort
    :returns: topologically sorted unit_list
    '''
    # NOTE: this might be potentially slow, as it uses a dictionaries of Units
    # implements a DFS on a custom stack
    state = {U: 0 for U in units_list}
    top_list = []
    unit_stack = []
    for U in units_list:
        if state[U] == 0:
            unit_stack.append(U)

        while unit_stack:
            V = unit_stack.pop()
            if state[V] == 0:
                state[V] = 1
                unit_stack.append(V)
                for W in V.parents:
                    if W in state and state[W] == 0:
                        unit_stack.append(W)
            elif state[V] == 1:
                top_list.append(V)
                state[V] = 2

    return top_list

def units_to_send_with_pid(poset, tops, pid):
    '''
    Determine which units created by pid to send to a poset which has the top known units of pid as given.
    Also make a list of units in tops we don't recognize.
    :param Poset poset: the poset which is supposed to be the source of the units
    :param list tops: the short representation of the receiving poset's top known units made by pid
    :param int pid: the id of the creator of units we are interested in
    :returns: units created by pid the other poset should receive to catch up to us, and a list of tops we don't recognize
    '''
    # NOTE: this can potentially be slowed down arbitrarily if forkers are not sufficiently muted.
    # TODO: ensure sufficient muting of forkers -- see create_unit's comments
    to_send = []
    local_max = poset.max_units_per_process[pid]
    local_max.sort(key=lambda U: U.height)
    min_remote_height = min([t[0] for t in tops]) if len(tops) > 0 else -1
    remote_hashes = [t[1] for t in tops]
    for U in local_max:
        possibly_send = []
        while U.height >= min_remote_height:
            if U.hash() in remote_hashes:
                to_send.extend(possibly_send)
                break
            else:
                possibly_send.append(U)
                U = U.self_predecessor
                if U is None:
                    break
    return to_send, [h for h in remote_hashes if h not in poset.units]

def _drop_to_height(units, height):
    if height == -1:
        return set()
    result = set()
    for U in units:
        while U.height > height:
            U = U.self_predecessor
        result.add(U)
    return result

def requested_units_to_send(poset, tops, requests):
    '''
    Collect units to send given the provided request.
    :param Poset poset: the poset which is supposed to be the source of the units
    :param list tops: the short representation of the receiving poset's top known units made by pid
    :param list requests: the hashes of the requested units
    :returns: units created by pid the other poset requested and has not yet seen
    '''
    if requests == []:
        return []
    to_send = []
    # NOTE: this fails terribly if we get a fake request
    requested = set(poset.units[h] for h in requests)
    known_remotes = set(poset.units[t[1]] for t in tops if t[1] in poset.units)
    operation_height = max(U.height for U in requested)
    known_remotes = _drop_to_height(known_remotes, operation_height)
    while requested:
        considered_requests = set(U for U in requested if U.height == operation_height)
        for U in considered_requests:
            to_send.append(U)
            if U not in known_remotes and U.self_predecessor is not None:
                requested.add(U.self_predecessor)
            requested.remove(U)
        operation_height -= 1
        known_remotes = _drop_to_height(known_remotes, operation_height)
    return to_send

def units_to_send(poset, info, requests = None):
    '''
    Determine which units to send to a poset which has the given info.
    :param Poset poset: the poset which is supposed to be the source of the units
    :param list info: the short representation of the receiving poset's state
    :param list requests: a list of explicitly requested units, per process
    :returns: units the other poset should receive to catch up to us
    '''
    if requests is None:
        requests = [[] for _ in info]
    to_send = []
    my_requests = []
    for pid in range(poset.n_processes):
        pid_to_send, pid_my_requests = units_to_send_with_pid(poset, info[pid], pid)
        to_send.extend(pid_to_send)
        my_requests.append(pid_my_requests)
        hashes_to_send = [U.hash() for U in pid_to_send]
        unfulfilled_requests = [h for h in requests[pid] if h not in hashes_to_send]
        to_send.extend(requested_units_to_send(poset, info[pid], unfulfilled_requests))
    return order_units_topologically(to_send), my_requests
