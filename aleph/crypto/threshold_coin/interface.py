class AbstractThresholdCoin:

    def __init__(self, dealer_id, n, k, validation_keys):
        """
        :param int dealer_id: identification number of a process dealing this coin, from 0 to n-1
        :param int n: number of processes
        :param int k: number of shares required to toss the coin
        :param validation_keys: list of n validation keys
        """
        self.dealer_id = dealer_id
        self.n = n
        self.k = k
        self.validation_keys = validation_keys


    def create_share(self, secret_key, nonce):
        """
        :param secret_key: secret key of a process creating this share
        :param int nonce: nonce for the share
        :returns: a tuple (dealer_id, nonce, coinshare)
        """
        pass


    def combine_shares(self, shares, nonce):
        """
        :param shares: a list of shares (tuples (dealer_id, nonce, coinshare))
        :param int nonce: nonce for the coin
        :returns: bool
        """
        pass
