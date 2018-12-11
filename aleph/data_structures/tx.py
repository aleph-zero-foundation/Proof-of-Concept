class Tx(object):
    '''This class stores a transactions issued by some user and is signed by the user'''

    __slots__ = ['issuer', 'signature', 'amount', 'receiver', 'index', 'validated', 'fee']

    def __init__(self, issuer, signature, amount, receiver, index):
        '''
        :param int issuer: public key of the issuer of the transaction
        :param int signature: signature made by the issuer of the transaction preventing forging transactions by Byzantine processes
        :param int amount: amount to be sent to the receiver
        :param int receiver: public key of the receiver of the transaction
        :param int index: a serial number of the transaction
        '''
        self.issuer = issuer
        self.signature = signature
        self.amount = amount
        self.receiver = receiver
        self.index = index


    @classmethod
    def from_dict(self, tx_dict):
        return Tx(tx_dict['issuer'],
                  tx_dict['signature'],
                  tx_dict['amount'],
                  tx_dict['receiver'],
                  tx_dict['index'])


    def __str__(self):
        # Required for temporary implementation of unit.hash()
        tx_string = ''
        tx_string += 'Issuer: ' + str(self.issuer) + '\n'
        tx_string += 'Receiver: ' + str(self.receiver) + '\n'
        tx_string += 'Amount: ' + str(self.amount) + '\n'
        tx_string += 'Index: ' + str(self.index) + '\n'
        return tx_string


    def __eq__(self, other):
        # self.validated field is ignored in this check
        return (isinstance(other, Tx) and self.issuer == other.issuer and self.amount == other.amount and self.signature == other.signature
                and self.receiver == other.receiver and self.index == other.index)

    def __hash__(self):
        return hash(str(self))


    def to_message(self):
        return tx_to_message(self.issuer, self.amount, self.receiver, self.index)


def tx_to_message(issuer, amount, receiver, index, fee):
    return str([issuer, amount, receiver, index, fee]).encode()
