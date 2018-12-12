from itertools import cycle
import hashlib

class CommonRandomPermutation:

    def __init__(self, public_keys, hashing_function = None):
        """
        :param public_keys: list of all public keys in lexicographic order
        :param hashing_function: hashing function used for generating common random permutations -- assumed to input and output a bytestring
        if None hashing_function is provided then it uses hashlib.sha512
        """
        self.public_keys = public_keys
        self.hashing_function = hashing_function

    def _xor(self, bytes1, bytes2):
        '''
        Returns a xor of two bytestrings bytes1, bytes2. The length of the result is the max of their lengths.
        If one of them is shorter it is rotated cyclically to obtain a string of matching length.
        '''
        assert len(bytes1) > 0 and len(bytes2) > 0, "An attempt to xor an empty bytestring"
        if len(bytes1) < len(bytes2):
            bytes1, bytes2 = bytes2, bytes1

        return bytes(a ^ b for (a, b) in zip(bytes1, cycle(bytes2)))

    def _hash(self, bytestring):
        if self.hashing_function is None:
            return hashlib.sha512(bytestring).digest()
        else:
            return self.hashing_function(bytestring)

    def __getitem__(self, k):
        """Return k-th common random permutation."""
        xor_all = bytes([0])
        for pk in self.public_keys:
            xor_all = self._xor(xor_all, pk)

        seeds = [self._hash(pk + (str(k).encode())) for pk in self.public_keys]
        seeds = [self._xor(xor_all, x) for x in seeds]

        indexed_seeds = zip(seeds, range(len(seeds)))
        indexed_seeds = sorted(list(indexed_seeds))
        return [ind for (seed, ind) in indexed_seeds]

