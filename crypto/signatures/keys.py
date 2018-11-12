import coincurve


class PrivateKey:
    def __init__(self):
        self.secret_key = coincurve.PrivateKey()
        self.type = 'coincurve.PrivateKey'

    def sign(self, message):
        '''
        :param bytes message: message to be signed
        '''
        return self.secret_key.sign(message)

class PublicKey:
    def __init__(self, secret_key):
        '''
        :param bytes secret: secret key used to generate PublicKey
        '''
        self.public_key = coincurve.PublicKey.from_secret(secret_key.secret_key.secret)
        self.type = 'coincurve.PublicKey'

    def verify_signature(self, signature, message):
        '''
        :param bytes signature: signature to verify
        :param bytes message: message that was supposedly signed
        '''
        return self.public_key.verify(signature, message)
