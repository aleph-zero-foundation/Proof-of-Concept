'''This module implements unit - a basic building block of Aleph protocol.'''
import hashlib
import pickle
import zlib

from aleph.config import PAIRING_GROUP

class Unit(object):
    '''This class is the building block for the poset'''

    __slots__ = ['creator_id', 'parents', 'txs', 'signature', 'coin_shares',
                 'level', 'floor', 'ceil', 'height', 'self_predecessor', 'hash_value']

    def __init__(self, creator_id, parents, txs, signature=None, coin_shares=None):
        '''
        :param int creator_id: indentification number of a process creating this unit
        :param list parents: list of parent units; first parent has to be above a unit created by the process creator_id
        :param list txs: list of transactions
        :param bytes signature: signature made by a process creating this unit preventing forging units by Byzantine processes
        :param list coin_shares: list of coin_shares if this is a prime unit, None otherwise
        '''
        self.creator_id = creator_id
        self.parents = parents
        self.signature = signature
        self.coin_shares = coin_shares or []
        self.level = None
        self.hash_value = None
        self.txs = zlib.compress(pickle.dumps(txs), level=4)
        #self.txs = txs


    def transactions(self):
        '''Iterate over transactions (instances of Tx class) belonging to this unit.'''
        return iter(pickle.loads(zlib.decompress(self.txs)))
        #return iter(self.txs)


    def parents_hashes(self):
        return [V.hash() for V in self.parents]


    def bytestring(self):
        '''Create a bytestring with all essential info about this unit for the purpose of signature creation and checking.'''
        separator = b'|'
        creator_bs =  str(self.creator_id).encode()
        parents_bs = separator.join([p.encode() for p in self.parents_hashes()])
        coin_bs = separator.join([PAIRING_GROUP.serialize(cs) for cs in self.coin_shares])
        return separator.join([creator_bs, parents_bs, coin_bs, self.txs])


    def serialize(self):
        '''Serialize this unit into bytestring that can be send via network.'''
        coin_shares = [PAIRING_GROUP.serialize(cs) for cs in self.coin_shares]
        state = (self.creator_id, self.parents_hashes(), self.txs, self.signature, coin_shares)
        return pickle.dumps(state)


    @classmethod
    def deserialize(cls, data, unit_hashes):
        '''Create new unit from bytestring data previously created with serialize().
        unit_hashes should be a dict with hashes as keys and units as values.
        '''
        creator, parents, txs, signature, coin_shares = pickle.loads(data)
        parents = [unit_hashes[p] for p in parents]
        coin_shares = [PAIRING_GROUP.deserialize(cs) for cs in coin_shares]
        ret = cls(int(creator), parents, [], signature, coin_shares)
        ret.txs = txs
        return ret


    def hash(self):
        '''Return the value of hash of this unit.'''
        if self.hash_value is not None:
            return self.hash_value
        self.hash_value = hashlib.sha512(self.bytestring()).hexdigest()
        return self.hash_value


    def __hash__(self):
        return hash(self.hash())


    def __eq__(self, other):
        return self.hash() == other.hash() #this is probably faster
        #return (isinstance(other, Unit) and self.creator_id == other.creator_id and self.parents_hashes() == other.parents_hashes() and self.txs == other.txs)


    def __str__(self):
        # create a string containing all the essential data in the unit
        str_repr =  str(self.creator_id)
        str_repr += str(self.parents_hashes())
        str_repr += str(self.txs)
        str_repr += str(self.coin_shares)
        return str_repr


    def __repr__(self):
        # create a string containing all the essential data in the unit
        str_repr =  str(self.creator_id)
        str_repr += str(self.parents_hashes())
        str_repr += str(self.txs)
        str_repr += str(self.coin_shares)
        #str_repr += str(self.height)
        #str_repr += str(self.level)?
        #str_repr += str(self.self_predecessor.hash())?
        return str_repr
