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

from time import time
from tqdm import tqdm
from random import randint, sample

from aleph.crypto import ThresholdCoin, generate_keys

n_parties, threshold = 1000, 667
VK, SKs = generate_keys(n_parties, threshold)

dealer_id = randint(0, n_parties)
TCs = [ThresholdCoin(dealer_id, pid, n_parties, threshold, SK, VK) for pid, SK  in enumerate(SKs)]

n_examples = 1000

results, times_gen, times_combine = [], [], []

for _ in tqdm(range(n_examples)):
    nonce = randint(0, 100000)

    # generate coin shares of all parties
    start = time()
    shares = [TC.create_coin_share(nonce) for TC in TCs]
    times_gen.append(time()-start)

    _shares = {i:shares[i] for i in sample(range(n_parties), threshold)}

    start = time()
    results.append(TCs[0].combine_coin_shares(_shares))
    times_combine.append(time()-start)
print('time needed for generating one share:', round(sum(times_gen)/len(times_gen)/1000, 4))
print('time needed for combining shares', round(sum(times_combine)/len(times_combine), 4))
print('mean value: ', sum(results)/len(results))
