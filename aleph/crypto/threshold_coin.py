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

from base64 import decodebytes
from random import randrange
from charm.core.math.pairing import hashPair


class ThresholdCoin:
    '''
    Implements dual threshold coin described in the whitepaper.

    :param int dealer_id: identification number of a process dealing this coin, from 0 to n-1
    :param int process_id: identification number of a process using this coin
    :param int n_processes: number of processes
    :param int threshold: number of shares required to toss the coin, has to satisfy n_processes//3 < threshold <= n_processes
    :param VerifyKey verification_key: key for combining shares
    :param SigningKey secret_key: key for generating a share of a coin toss
    '''
 
    def __init__(self, dealer_id, process_id, n_processes, threshold, secret_key, verification_key):
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

        msg_hash = self.verification_key.hash_fct(str(randrange(0, 1000)))
        coin_share = self.secret_key.generate_share(msg_hash)

        return self.verification_key.verify_share(coin_share, self.process_id, msg_hash)

    def create_coin_share(self, nonce):
        '''
        :param int nonce: nonce for the coin share
        :returns: coin share for the nonce
        '''
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

        msg_hash = self.verification_key.hash_fct(str(nonce))

        return self.verification_key.verify_share(coin_share, process_id, msg_hash)


    def combine_coin_shares(self, shares, nonce):
        '''
        Combines the coin shares by forming a threshold signature and taking its 1st bit, subsequently it verifies the result.
        NOTE: combining shares should always succeed except when some of the shares were invalid or the dealer was dishonest, in which case the toss might be biased and should ideally be discarded

        :param dict shares: keys are processes ids, values are shares (group G1 elements)
        :param string nonce: the nonce the shares were created for -- necessary for verifying the result of combining
        :returns:  pair (int, bool) :  (coin toss in {0,1}) , (whether combining shares was succesful)
        '''

        # there are enough shares of a coin
        assert len(shares) == self.threshold, 'Not enough shares for combining'

        signature = self.verification_key.combine_shares(shares)
        hex_string = hashPair(signature).decode()
        # we just use the first bit as the coin toss
        coin_value = bytes.fromhex(hex_string)[0] % 2

        # verify the result
        nonce_hash = self.verification_key.hash_fct(nonce)
        correctness = self.verification_key.verify_signature(signature, nonce_hash)

        return (coin_value, correctness)
