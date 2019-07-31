class Tx(object):
    '''A class representing a single transaction, that is an act of sending some number of tokens from one user to another.'''

    __slots__ = ['issuer', 'receiver', 'amount']

    def __init__(self, issuer, receiver, amount):
        '''
        :param str issuer: public key of the issuer of the transaction
        :param str receiver: public key of the receiver of the transaction
        :param int amount: amount to be sent to the receiver
        '''
        self.issuer = issuer
        self.receiver = receiver
        self.amount = amount


    def __getstate__(self):
        return (self.issuer, self.receiver, self.amount)


    def __setstate__(self, state):
        self.issuer, self.receiver, self.amount = state


    def __str__(self):
        tx_string =  'Issuer: ' + str(self.issuer) + '\n'
        tx_string += 'Receiver: ' + str(self.receiver) + '\n'
        tx_string += 'Amount: ' + str(self.amount) + '\n'
        return tx_string

    __repr__ = __str__


    def __eq__(self, other):
        return (isinstance(other, Tx) and self.issuer == other.issuer
                and self.receiver == other.receiver
                and self.amount == other.amount)


    def __hash__(self):
        return hash(str(self))
