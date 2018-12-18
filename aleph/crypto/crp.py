from .xor import xor

import hashlib

class CommonRandomPermutation:

    def __init__(self, public_keys_hex, hashing_function = None):
        """
        :param public_keys: list of all public keys in hex format
        :param hashing_function: hashing function used for generating common random permutations -- assumed to input and output a bytestring
        if None hashing_function is provided then it uses hashlib.sha512
        """
        self.public_keys = [bytes.fromhex(key_hex) for key_hex in public_keys_hex]
        self.hashing_function = hashing_function

    def _hash(self, bytestring):
        if self.hashing_function is None:
            return hashlib.sha512(bytestring).digest()
        else:
            return self.hashing_function(bytestring)

    def __getitem__(self, k):
        """Return k-th common random permutation."""
        xor_all = bytes([0])
        for pk in self.public_keys:
            xor_all = xor(xor_all, pk)

        seeds = [self._hash(pk + (str(k).encode())) for pk in self.public_keys]
        seeds = [xor(xor_all, x) for x in seeds]

        indexed_seeds = zip(seeds, range(len(seeds)))
        indexed_seeds = sorted(list(indexed_seeds))
        return [ind for (seed, ind) in indexed_seeds]

