from aleph.crypto.threshold_coin import ThresholdCoin
from aleph.crypto.signatures.threshold_signatures import generate_keys

import random


class TestThresholdCoin():
    def test_one(self):
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

        _shares = {i:shares[i] for i in random.sample(range(n_parties), threshold)}

        pid = random.randrange(n_parties)
        assert TCs[pid].combine_coin_shares(_shares, str(nonce))[0] in [0, 1]
