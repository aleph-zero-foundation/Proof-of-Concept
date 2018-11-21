from time import time
from tqdm import tqdm
from random import randint, sample

from aleph.crypto.threshold_coin import ThresholdCoin
from aleph.crypto.signatures.threshold_signatures import generate_keys

n_parties, threshold = 1000, 667
VK, SKs = generate_keys(n_parties, threshold)

dealer_id = randint(0, n_parties)
TCs = [ThresholdCoin(dealer_id, n_parties, threshold, VK, SK) for SK in SKs]

n_examples = 1000

results, times_gen, times_combine = [], [], []

for _ in tqdm(range(n_examples)):
    nonce = randint(0, 100000)

    # generate coin shares of all parties
    start = time()
    shares = [TC.create_share(nonce) for TC in TCs]
    times_gen.append(time()-start)

    start = time()
    _shares = {i:shares[i] for i in sample(range(n_parties), threshold)}
    times_combine.append(time()-start)

    results.append(TCs[0].combine_shares(_shares))
print('time needed for generating one share:', round(sum(times_gen)/len(times_gen)/1000, 4))
print('time needed for combining shares', round(sum(times_combine)/len(times_combine), 4))
print('mean value: ', sum(results)/len(results))
