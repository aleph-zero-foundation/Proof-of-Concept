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

from random import sample

from aleph.crypto.threshold_signatures import generate_keys


def test_combine_signature():
    '''
    Performs the following:
        - creates a threshold signature (5 out of 10) for a message.
        - verifies all the shares
        - combines all the shares into a signature
        - verifies the signature
    '''
    n_parties, threshold = 10, 5
    VK, SKs = generate_keys(n_parties, threshold)

    msg = 'there is no spoon'
    msg_hash = VK.hash_msg(msg)

    # generate signature shares of all parties
    shares = [SK.generate_share(msg_hash) for SK in SKs]
    _shares = {i: shares[i] for i in sample(range(n_parties), threshold)}

    # check if all shares are valid
    for i, share in _shares.items():
        assert VK.verify_share(share, i, msg_hash)

    # combine shares and check if the signature is valid
    signature = VK.combine_shares(_shares)

    assert VK.verify_signature(signature, msg_hash)
