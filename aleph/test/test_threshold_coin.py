from aleph.crypto.threshold_coin import ThresholdCoin
from aleph.crypto.signatures.threshold_signatures import generate_keys

import random


class TestThresholdCoin():
    def test_one(self):
        n_parties, threshold = 10, 5
        VK, SKs = generate_keys(n_parties, threshold)

        dealer_id = random.randint(0, n_parties)
        TCs = [ThresholdCoin(dealer_id, n_parties, threshold, VK, SK) for SK in SKs]

        nonce = random.randint(0, 100000)

        # generate coin shares of all parties
        shares = [TC.create_share(nonce) for TC in TCs]
        _shares = {i:shares[i] for i in random.sample(range(n_parties), threshold)}

        assert TCs[0].combine_shares(_shares) in [0, 1]


