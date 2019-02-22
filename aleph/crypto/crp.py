from .byte_utils import xor
from .byte_utils import sha3_hash


class CommonRandomPermutation:

    def __init__(self, public_keys_hex, hashing_function = None):
        """
        :param public_keys: list of all public keys in hex format
        :param hashing_function: hashing function used for generating common random permutations -- assumed to input and output a bytestring
        if None hashing_function is provided then it uses sha3_hash i.e. hashlib.sha512
        """
        self.public_keys_hex = public_keys_hex
        self.hashing_function = hashing_function
        self.cache = {}
        self.cache_size = 20


    def _hash(self, bytestring):
        if self.hashing_function is None:
            return sha3_hash(bytestring)
        else:
            return self.hashing_function(bytestring)

    def add_to_cache(self, k, permutation):
        # adds an element to cache and shrinks the size of cache if the size becomes > 2*cache_size
        self.cache[k] = permutation
        if len(self.cache) > 2*self.cache_size:
            # the cache grew too large -- fetch its elements sorted from highest indices to lowest
            cache_items = list(reversed(sorted(self.cache.items())))
            # remove all but cache_size largest indices
            self.cache = dict(cache_items[:self.cache_size])

    def __getitem__(self, k):
        """Return k-th common random permutation."""
        if k in self.cache:
            return self.cache[k]

        xor_all = bytes([0])
        for pk in self.public_keys_hex:
            xor_all = xor(xor_all, pk)

        seeds = [self._hash(pk + (str(k).encode())) for pk in self.public_keys_hex]
        seeds = [xor(xor_all, x) for x in seeds]

        indexed_seeds = zip(seeds, range(len(seeds)))
        indexed_seeds = sorted(list(indexed_seeds))
        permutation = [ind for (seed, ind) in indexed_seeds]
        self.add_to_cache(k, permutation)
        return permutation

