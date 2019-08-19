'''
    This is a Proof-of-Concept implementation of Aleph Zero consensus protocol.
    Copyright (C) 2019 Aleph Zero Team
    
    This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
    This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
    
    You should have received a copy of the GNU General Public License
    along with this program. If not, see <http://www.gnu.org/licenses/>.
'''

import asyncio
import multiprocessing
import random

from aleph.data_structures import Unit
from aleph.process import Process
from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.actions import create_unit


def translate_parents_and_copy(U, hashes_to_units):
    '''
    Takes a unit from the poset of a process A and the mapping hashes->Units for the poset for process B.
    Returns a new unit (with correct references to corresponding parent units in B) to be added to B's poset.
    The new unit has all the data in the floor/level/... fields erased.
    '''
    parent_hashes = [V.hash() for V in U.parents]
    parents = [hashes_to_units[V] for V in parent_hashes]
    U_new = Unit(U.creator_id, parents, U.transactions(), U.signature, U.coin_shares)
    return U_new


n_processes = 16
n_units = 1000
use_tcoin = True
processes = []
host_ports = [8900+i for i in range(n_processes)]
addresses = [('127.0.0.1', port) for port in host_ports]
recv_addresses = [('127.0.0.1', 9100+i) for i in range(n_processes)]

signing_keys = [SigningKey() for _ in range(n_processes)]
public_keys = [VerifyKey.from_SigningKey(sk) for sk in signing_keys]

for process_id in range(n_processes):
    sk = signing_keys[process_id]
    pk = public_keys[process_id]
    new_process = Process(n_processes, process_id, sk, pk, addresses, public_keys, recv_addresses[process_id], None, use_tcoin)
    processes.append(new_process)


for unit_no in range(n_units):
    while True:
        creator_id = random.choice(range(n_processes))
        process = processes[creator_id]
        new_unit = create_unit(process.poset, creator_id, [])
        if new_unit is None:
            continue

        process.poset.prepare_unit(new_unit)
        assert process.poset.check_compliance(new_unit), "A unit created by this process is not passing the compliance test!"
        process.sign_unit(new_unit)
        process.add_unit_to_poset(new_unit)

        for process_id in range(n_processes):
            if process_id != creator_id:
                hashes_to_units = processes[process_id].poset.units
                U = translate_parents_and_copy(new_unit, hashes_to_units)
                processes[process_id].add_unit_to_poset(U)
        break

    if unit_no%50 == 0:
        print(f"Adding unit no {unit_no} out of {n_units}.")
