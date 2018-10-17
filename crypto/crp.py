class CommonRandomPermutation:

    def __init__(self, public_keys, hashing_function):
        """
        :param public_keys: list of all public keys in lexicographic order
        :param hashing_function: hashing function used for generating common random permutations
        """
        self.public_keys = public_keys
        self.hashing_function = hashing_function

    def __getitem__(self, k):
        """Return k-th common random permutation."""
        pass
