from functools import reduce

from charm.toolbox.pairinggroup import ZR, G1, pair

from aleph.config import PAIRING_GROUP, GENERATOR

# The implementation is based on: Boldyreva, 2002 https://eprint.iacr.org/2002/118.pdf
# Possible alternative implementation: Shoup, 2000 http://eprint.iacr.org/1999/011


def generate_keys(n_parties, threshold):
    '''
    Generates one verification key and n_parties secret keys.
    :param int n_parties: number of parties that need secret keys
    :param int threshold: number of signature shares required for generating a signature
    '''

    group = PAIRING_GROUP

    # pick a generator of the group
    gen = GENERATOR

    # pick a set of coefficients
    coef = group.random(ZR, threshold)
    secret = coef[-1]

    # generate secret keys
    sks = [_poly(coef, x) for x in range(1, n_parties+1)]

    # generate underlying verification keys
    vk = gen ** secret
    vks = [gen ** scr for scr in sks]

    verification_key = VerificationKey(threshold, vk, vks)
    secret_keys = [SecretKey(sk) for sk in sks]

    return verification_key, secret_keys


class VerificationKey:
    '''
    An object used for verifying shares and signatures and for combining shares into signatures.
    '''

    def __init__(self, threshold, vk, vks):
        '''
        :param int threshold: number of signature shares needed to generate a signature
        :param int vk: global verification key
        :param list vks: verification keys corresponding to secret keys of all parties
        '''
        self.threshold = threshold
        self.vk = vk
        self.vks = vks

        self.group = PAIRING_GROUP
        self.gen = GENERATOR

    def hash_fct(self, msg):
        '''
        Hash function used for hashing messages into group G1.
        :param string msg: message to be hashed
        :returns: element of G1 group
        '''

        return self.group.hash(msg, G1)

    def lagrange(self, S, i):
        '''
        Lagrange interpolation.
        :param list S: list of values for numerator
        :param int i: special value for denumerator
        '''

        one = self.group.init(ZR, 1)
        S = sorted(S)
        num = reduce(lambda x, y: x*y, [0 - j - 1 for j in S if j != i], one)
        den = reduce(lambda x, y: x*y, [i - j     for j in S if j != i], one)

        return num/den

    def verify_share(self, share, i, msg_hash):
        '''
        Verifies if a share generated by i-th party is valid.
        :param int share: share of a signature of a hash of a message
        :param int i: index number of a party
        :param int msg_hash: hash of a message that is signed
        '''
        return pair(share, self.gen) == pair(msg_hash, self.vks[i])

    def verify_signature(self, signature, msg_hash):
        '''
        Verifies if signature is valid.
        :param int signature: signature of msg_hash to be chacked
        :param int msg_hash: hash of a message corresponding to signature.
        '''
        return pair(signature, self.gen) == pair(msg_hash, self.vk)

    def combine_shares(self, shares):
        '''
        Combines shares into a signature of a message.
        :param dict shares: shares of a signature to be produced
        '''
        assert len(shares) == self.threshold
        R = shares.keys()
        return reduce(lambda x,y: x*y, [share ** self.lagrange(R, i) for i, share in shares.items()], 1)

    def hash_msg(self, msg):
        '''
        Hashes a message before signing.
        :param bytes msg: message to be hashed
        '''
        return self.hash_fct(msg)


class SecretKey:
    '''
    An object used for generating shares of a signature of a message.
    '''

    def __init__(self, sk):
        '''
        :param int sk: secret used for signing
        '''

        self.sk = sk

    def generate_share(self, msg_hash):
        '''
        Generates a share of a signature of a hash of a message.
        :param int msg_hash: hash of a message which signature share is generated
        '''
        return msg_hash ** self.sk

def _poly(coefs, x):
    '''
    Evaluates a polynomial given by coefficients at some point.
    :param list coefs: list of coefficients
    :param int x: evaluation point
    '''
    return reduce(lambda y, coef: x*y+coef, coefs, 0)
