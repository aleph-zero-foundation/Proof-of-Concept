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

from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.data_structures import Unit
from aleph.process import Process


def test_signing():
    '''
    Tests whether a correct signature of a Unit.bytestring() is positively verified.
    '''
    sk = SigningKey()
    vk = VerifyKey.from_SigningKey(sk)
    U = Unit(0,[],[])
    msg = U.bytestring()
    signature = sk.sign(msg)
    assert vk.verify_signature(signature, msg)

def test_process_signing():
    '''
    Tests whether a process correctly signs a Unit.
    '''
    process_id = 0
    n_processes = 100
    sk = SigningKey()
    vk = VerifyKey.from_SigningKey(sk)
    dummy_keys = [VerifyKey.from_SigningKey(SigningKey()) for _ in range(n_processes)]
    dummy_keys[0] = vk
    dummy_addresses = [(None, None) for _ in range(n_processes)]
    process = Process(n_processes, process_id, sk, vk, dummy_addresses, dummy_keys, None)
    U = Unit(0,[],[])
    process.sign_unit(U)

    msg = U.bytestring()
    assert vk.verify_signature(U.signature, msg)

