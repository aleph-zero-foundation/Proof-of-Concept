from charm.core.math.pairing import hashPair
from base64 import decodebytes
from random import randrange



class ThresholdCoin:

    def __init__(self, dealer_id, process_id, n_processes, threshold, secret_key, verification_key):
        """
        :param int dealer_id: identification number of a process dealing this coin, from 0 to n-1
        :param int process_id: identification number of a process using this coin
        :param int n_processes: number of processes
        :param int threshold: number of shares required to toss the coin
        :param verification_key: key for combining shares
        :param secret_key: key for generating a share of a coin toss
        """
        self.dealer_id = dealer_id
        self.process_id = process_id
        self.n_processes = n_processes
        self.threshold = threshold
        self.secret_key = secret_key
        self.verification_key = verification_key


    def check_validity(self):
        '''
        Checks if this threshold coin is valid.
        '''

        msg_hash = self.verification_key.hash_fct(str(randrange(0,1000)))
        coin_share = self.secret_key.generate_share(msg_hash)

        return self.verification_key.verify_share(coin_share, self.process_id, msg_hash)


    def create_coin_share(self, nonce):
        """
        :param int nonce: nonce for the coin share
        :returns: coin share for the nonce
        """
        msg_hash = self.verification_key.hash_fct(str(nonce))
        coin_share = self.secret_key.generate_share(msg_hash)

        return coin_share


    def verify_coin_share(self, coin_share, process_id, nonce):
        '''
        :param CoinShare coin_share: coin_share which validity is checked
        :param int process_id: identification number of a process that generated the coin_share
        :param int nonce: nonce for which the coin_share was generated
        :returns: True if coin_share is valid and False otherwise
        '''
        # TODO there is alternative implementation based on RSA that supports checking if
        # the coin share was generated by a particular process

        msg_hash = self.verification_key.hash_fct(str(nonce))

        return self.verification_key.verify_share(coin_share, process_id, msg_hash)


    def combine_coin_shares(self, shares, verify = False, nonce = None):
        """
        Assumes that all shares are valid (the)
        :param dict shares: keys are processes ids, values are shares (tuples (dealer_id, nonce, coin_shares))
        :param bool verify: make True to verify whether the combined signature is correct, in this case the nonce parameter is also mandatory
        :returns:   if verify = False: bool (coin toss),
                    if verify = True:  pair (bool, bool) :  (coin toss) , (whether combining shares was succesful)
        NOTE: combining shares should always succeed except when some of the shares were invalid,
              in which case the toss might be biased and should ideally be discarded
        """
        # there are enough shares of a coin
        assert len(shares) == self.threshold


        signature = self.verification_key.combine_shares(shares)
        hex_string = hashPair(signature).decode()
        # we just use the first bit
        coin_value = bytes.fromhex(hex_string)[0]%2

        if verify:
            assert nonce is not None, "Verification of coin toss requested but nonce not provided."
            nonce_hash = self.verification_key.hash_fct(nonce)
            correctness = self.verification_key.verify_signature(signature, nonce_hash)
            return (coin_value, correctness)
        else:
            return coin_value