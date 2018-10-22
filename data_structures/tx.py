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
