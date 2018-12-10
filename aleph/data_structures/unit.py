'''This module implements unit - a basic building block of Aleph protocol.'''
import hashlib

class Unit(object):
    '''This class is the building block for the poset'''

    __slots__ = ['creator_id', 'parents', 'txs', 'signature', 'coinshares',
                 'level', 'floor', 'ceil', 'height', 'self_predecessor', 'hash_value']

    def __init__(self, creator_id, parents, txs, signature=None, coinshares=None, level=None):
        '''
        :param int creator_id: indentification number of a process creating this unit
        :param list parents: list of hashes of parent units; first parent has to be above a unit created by the process creator_id
        :param list txs: list of transactions
        :param int signature: signature made by a process creating this unit preventing forging units by Byzantine processes
        :param list coinshares: list of coinshares if this is a prime unit, null otherwise
        '''
        self.creator_id = creator_id
        self.parents = parents
        self.txs = txs
        self.signature = signature
        self.coinshares = coinshares
        self.level = level
        self.hash_value = None


    def hash(self):
        '''
        Hashing function used to hide addressing differences among the committee
        '''
        # TODO: this is only a temporary implementation!
        # TODO: need to be updated at some point!
        # TODO: should coinshares be hashed?
        # TODO: should order of parents (or txs) influence the hash value?

        if self.hash_value is not None:
            return self.hash_value

        self.hash_value = hashlib.sha512(str(self).encode()).hexdigest()
        return self.hash_value


    def parents_hashes(self):
        return [V.hash() for V in self.parents]


    def to_message(self):
        '''Generates message used for signing units'''
        return unit_to_message(self.creator_id, self.parents_hashes(), self.txs, self.coinshares)

    def __hash__(self):
        return hash(self.hash())


    def __eq__(self, other):
        return (isinstance(other, Unit) and self.creator_id == other.creator_id and
               self.parents_hashes() == other.parents_hashes() and
               set(map(str, self.txs)) == set(map(str, other.txs)))


    def __str__(self):
        # create a string containing all the essential data in the unit
        str_repr = ''
        str_repr += str(self.creator_id)
        str_repr += str(self.parents_hashes())
        str_repr += str(self.txs)
        str_repr += str(self.coinshares)

        return str_repr


    def __repr__(self):
        # create a string containing all the essential data in the unit
        str_repr = ''
        str_repr += str(self.creator_id)
        str_repr += str(self.parents_hashes())
        str_repr += str(self.txs)
        str_repr += str(self.coinshares)
        str_repr += str(self.level)
        str_repr += str(self.height)
        #str_repr += str(self.self_predecessor.hash())
        str_repr += str(self.floor)
        str_repr += str(self.ceil)

        return str_repr


def unit_to_message(creator_id, parents, txs, coinshares):
    return str([creator_id, parents, txs, coinshares]).encode()
