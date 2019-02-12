import logging
import random
import socket

from aleph.const import LOGGER_NAME
from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.data_structures import Unit
from aleph.process import Process
from byzantine_process import ByzantineProcess


def get_last_forking_units(last_position, process):
    return process.forking_units[last_position:]


def translate_parents_and_copy(U, hashes_to_units):
    '''
    Takes a unit from the poset of a process A and the mapping hashes->Units for the poset for process B. Returns a new unit
    (with correct references to corresponding parent units in B) to be added to B's poset. The new unit has all the data in the
    floor/ceil/level/... fields erased.
    '''
    parent_hashes = [V.hash() for V in U.parents]
    parents = [hashes_to_units[V] for V in parent_hashes]
    U_new = Unit(U.creator_id, parents, U.transactions(), U.signature, U.coin_shares)
    return U_new


def is_byzantine(id):
    return id == 0


def byzantine_process_builder(n_processes, process_id, sk, pk, addresses,
                             public_keys, recv_address, userDB=None,
                             validation_method='LINEAR_ORDERING'):
    builder = Process
    if is_byzantine(process_id):
        builder = ByzantineProcess

    return builder(n_processes, process_id, sk, pk, addresses, public_keys, recv_address, userDB, validation_method)


def execute_byzantine_test(process_builder=Process):
    logger = logging.getLogger(LOGGER_NAME)
    n_processes = 16
    n_units = 1000
    processes = []
    host_ports = [8900+i for i in range(n_processes)]
    local_ip = socket.gethostbyname(socket.gethostname())
    addresses = [(local_ip, port) for port in host_ports]
    recv_addresses = [(local_ip, 9100+i) for i in range(n_processes)]

    signing_keys = [SigningKey() for _ in range(n_processes)]
    public_keys = [VerifyKey.from_SigningKey(sk) for sk in signing_keys]

    for process_id in range(n_processes):
        sk = signing_keys[process_id]
        pk = public_keys[process_id]
        recv_address = recv_addresses[process_id]
        new_process = process_builder(n_processes,
                                      process_id,
                                      sk, pk,
                                      addresses,
                                      public_keys,
                                      recv_address,
                                      userDB=None,
                                      validation_method='LINEAR_ORDERING')
        processes.append(new_process)

    number_of_forking_units = 0
    for unit_no in range(n_units):
        while True:
            creator_id = random.choice(range(n_processes))
            process = processes[creator_id]
            units_before_adding = len(process.poset.units)
            new_unit = process.create_unit([])
            if new_unit is None:
                continue

            process.poset.prepare_unit(new_unit)
            if not process.poset.check_compliance(new_unit):
                raise Exception('a unit created by a process is not passing the compliance test')
            process.sign_unit(new_unit)
            if not process.add_unit_to_poset(new_unit):
                process.add_unit_to_poset(new_unit)
                raise Exception("a newly created unit can't be added to its creator node")
            new_units = [new_unit]
            if is_byzantine(creator_id):
                forking_units = get_last_forking_units(number_of_forking_units, process)
                new_units = new_units + forking_units
                number_of_forking_units += len(forking_units)

            units_count = len(process.poset.units) - len(new_units)
            if units_count != units_before_adding:
                raise Exception('incorrect number of units')

            for process_id in range(n_processes):
                if process_id == creator_id:
                    continue
                other_process = processes[process_id]
                if len(other_process.poset.units) != units_count:
                    raise Exception('wrong number of units at some other process: %d != %d' %
                                    (units_count, len(other_process.poset.units)))
                logger.debug('number of units before update: %d' % len(other_process.poset.units))
                for U in new_units:
                    U = translate_parents_and_copy(U, other_process.poset.units)
                    if not other_process.add_unit_to_poset(U):
                        raise Exception("a newly created unit can't be added to some other node")
                logger.debug(f'number of units after update: {len(other_process.poset.units)}')
                if len(other_process.poset.units) != len(process.poset.units):
                    raise Exception('incorrect number of nodes after update:'
                                     '{len(other_process.poset.units) != len(process.poset.units)}')

            break

        if unit_no % 50 == 0:
            print(f"Adding unit no {unit_no} out of {n_units}.")

        # dag, translation = dag_utils.dag_from_poset(process.poset)
        # plot_dag(dag)


if __name__ == "__main__":
    execute_byzantine_test(byzantine_process_builder)
