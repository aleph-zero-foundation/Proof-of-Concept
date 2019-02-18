import logging
import random
import socket

from aleph.const import LOGGER_NAME
from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.process import Process
from byzantine_process import ByzantineProcess


def is_byzantine(id):
    return id == 0


def byzantine_process_builder(n_processes, process_id, sk, pk, addresses,
                              public_keys, recv_address, userDB=None,
                              validation_method='LINEAR_ORDERING'):
    builder = Process
    if is_byzantine(process_id):
        builder = ByzantineProcess

    return builder(n_processes, process_id, sk, pk, addresses, public_keys, recv_address, userDB, validation_method)


def execute_byzantine_test(process_builder=byzantine_process_builder):
    '''
    Executes a simple test consisting of creation of a given number of units and manual addition of each of them to every
    instance of the Process class. Some of the processes are of the byzantine type (i.e. creating forks).
    :param function process_builder: factory method for the instances of the class Process.
    '''
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
                forking_units = process.forking_units[number_of_forking_units:]
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
                    U = ByzantineProcess.translate_unit(U, other_process)
                    if not other_process.add_unit_to_poset(U):
                        raise Exception("a newly created unit can't be added to some other node")
                logger.debug(f'number of units after update: {len(other_process.poset.units)}')
                if len(other_process.poset.units) != len(process.poset.units):
                    raise Exception('incorrect number of nodes after update:'
                                    '{len(other_process.poset.units) != len(process.poset.units)}')

            break

        if unit_no % 50 == 0:
            print(f"Adding unit no {unit_no} out of {n_units}.")


if __name__ == "__main__":
    execute_byzantine_test(byzantine_process_builder)
