class Tx(object):
    '''This class stores a transactions issued by some user and is signed by the user'''

    __slots__ = ['issuer', 'receiver', 'amount', 'index']#, 'signature']

    def __init__(self, issuer, receiver, amount, index):#, signature=None):
        '''
        :param str issuer: public key of the issuer of the transaction
        :param str receiver: public key of the receiver of the transaction
        :param int amount: amount to be sent to the receiver
        :param int index: a serial number of the transaction
        :param bytes signature: signature made by the issuer of the transaction preventing forging transactions by Byzantine processes
        '''
        self.issuer = issuer
        self.receiver = receiver
        self.amount = amount
        self.index = index
        #self.signature = signature


    def __getstate__(self):
        return (self.issuer, self.receiver, self.amount, self.index)


    def __setstate__(self, state):
        self.issuer, self.receiver, self.amount, self.index = state


    def __str__(self):
        tx_string =  'Issuer: ' + str(self.issuer) + '\n'
        tx_string += 'Receiver: ' + str(self.receiver) + '\n'
        tx_string += 'Amount: ' + str(self.amount) + '\n'
        tx_string += 'Index: ' + str(self.index) + '\n'
        return tx_string


    __repr__ = __str__


    def __eq__(self, other):
        return (isinstance(other, Tx) and self.issuer == other.issuer
                and self.receiver == other.receiver
                and self.amount == other.amount
                and self.index == other.index)
                #and self.signature == other.signature)


    def __hash__(self):
        return hash(str(self))

