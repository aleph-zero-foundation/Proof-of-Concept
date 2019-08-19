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

import random
import string

from aleph.crypto import SigningKey, VerifyKey

def test_true():
    '''
    Test whether correct message signatures are accepted.
    '''
    sk = SigningKey()
    vk = VerifyKey.from_SigningKey(sk)
    for _ in range(10):
        n = random.randint(50,200)
        msg = ''.join(random.choices(string.printable, k=n))
        if random.randint(0,1):
            msg = msg.encode()
        sign = sk.sign(msg)
        assert vk.verify_signature(sign, msg)


def test_false():
    '''
    Test whether incorrect message signatures are rejected.
    '''
    sk = SigningKey()
    vk = VerifyKey.from_SigningKey(sk)
    for _ in range(10):
        n = random.randint(50,200)
        msg = ''.join(random.choices(string.printable, k=n))
        if random.randint(0,1):
            msg = msg.encode()
        sign = sk.sign(msg)
        # drop one random character in the message to produce a different message
        k = random.randint(0, n-1)
        msg = msg[:k] + msg[k+1:]
        assert not vk.verify_signature(sign, msg)
