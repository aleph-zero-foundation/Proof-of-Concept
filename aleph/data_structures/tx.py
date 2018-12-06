class Tx(object):
    '''This class stores a transactions issued by some user and is signed by the user'''

    __slots__ = ['issuer', 'signature', 'amount', 'receiver', 'index', 'validated', 'fee']

    def __init__(self, issuer, signature, amount, receiver, index, validated, fee):
        '''
        :param int issuer: public key of the issuer of the transaction
        :param int signature: signature made by the issuer of the transaction preventing forging transactions by Byzantine processes
        :param int amount: amount to be sent to the receiver
        :param int receiver: public key of the receiver of the transaction
        :param int index: a serial number of the transaction
        :param bool validated: indicates whether the transaction got validated
        :param int fee: amount paid to the committee for processing the transaction
        '''
        self.issuer = issuer
        self.signature = signature
        self.amount = amount
        self.receiver = receiver
        self.index = index
        self.validated = validated
        self.fee = fee


    def __str__(self):
        # Required for temporary implementation of unit.hash()
        tx_string = ''
        tx_string += 'Issuer: ' + str(self.issuer) + '\n'
        tx_string += 'Receiver: ' + str(self.receiver) + '\n'
        tx_string += 'Amount: ' + str(self.amount) + '\n'
        tx_string += 'Index: ' + str(self.index) + '\n'
        tx_string += 'Fee: ' + str(self.fee) + '\n'
        return tx_string

    def to_message(self):
        return tx_to_message(self.issuer, self.amount, self.receiver, self.index, self.fee)


def tx_to_message(issuer, amount, receiver, index, fee):
    return str([issuer, amount, receiver, index, fee]).encode()
