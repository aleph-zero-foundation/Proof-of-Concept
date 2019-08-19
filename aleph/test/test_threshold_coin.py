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

from aleph.crypto.threshold_coin import ThresholdCoin
from aleph.crypto.threshold_signatures import generate_keys


def test_tcoin():
    '''
    A simple test for generating and extracting threshold coin tosses.
    '''
    n_parties, threshold = 10, 5
    VK, SKs = generate_keys(n_parties, threshold)

    dealer_id = random.randint(0, n_parties)
    TCs = [ThresholdCoin(dealer_id, pid, n_parties, threshold, SK, VK) for pid, SK in enumerate(SKs)]

    nonce = random.randint(0, 100000)

    # generate coin shares of all parties
    shares = [TC.create_coin_share(nonce) for TC in TCs]

    # verify all shares
    for i, share in enumerate(shares):
        pid = random.randrange(n_parties)
        assert TCs[pid].verify_coin_share(share, i, nonce)

    _shares = {i: shares[i] for i in random.sample(range(n_parties), threshold)}

    pid = random.randrange(n_parties)
    assert TCs[pid].combine_coin_shares(_shares, str(nonce))[0] in [0, 1]
