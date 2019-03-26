from .byte_utils import xor
from .byte_utils import sha3_hash


class CommonRandomPermutation:
    '''This class represents common random permutation defined in the whitepaper.'''

    def __init__(self, public_keys_hex, hashing_function=None):
        '''
        :param list public_keys: list of all public keys in hex format
        :param function hashing_function: hashing function used for generating common random permutations -- assumed to take input and output a bytestring
        if None hashing_function is provided then it uses sha3_hash i.e. hashlib.sha512
        '''
        self.public_keys_hex = public_keys_hex
        self.hashing_function = hashing_function
        self.cache = {}
        self.cache_size = 20

    def _hash(self, bytestring):
        if self.hashing_function is None:
            return sha3_hash(bytestring)
        return self.hashing_function(bytestring)

    def add_to_cache(self, level, permutation):
        '''
        Adds an element to cache and shrinks the size of cache if the size becomes > 2*cache_size
        :param int level: level for which permutation is cached
        :param list permutation: permutation of the set n to be cached
        '''

        self.cache[level] = permutation
        if len(self.cache) > 2*self.cache_size:
            # the cache grew too large -- fetch its elements sorted from highest indices to lowest
            cache_items = list(reversed(sorted(self.cache.items())))
            # remove all but cache_size largest indices
            self.cache = dict(cache_items[:self.cache_size])

    def index_of(self, item, level):
        '''
        Outputs the position in the permutation crp[level] of item.
        :param int item: the number whose position we seek
        :param int level: the level of the permutation of interest
        :returns: the position of item in the permutation at this level
        '''
        sigma = self.__getitem__(level)
        return sigma.index(item)

    def __getitem__(self, level):
        '''
        Returns common random permutation for level level.
        :param int level: level for which the permutation is returned
        '''
        if level in self.cache:
            return self.cache[level]

        xor_all = bytes([0])
        for pk in self.public_keys_hex:
            xor_all = xor(xor_all, pk)

        seeds = [self._hash(pk + (str(level).encode())) for pk in self.public_keys_hex]
        seeds = [xor(xor_all, x) for x in seeds]

        indexed_seeds = zip(seeds, range(len(seeds)))
        indexed_seeds = sorted(list(indexed_seeds))
        permutation = [ind for (seed, ind) in indexed_seeds]
        self.add_to_cache(level, permutation)
        return permutation
