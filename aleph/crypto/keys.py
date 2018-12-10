import nacl.signing


class SigningKey:
    def __init__(self):
        self.secret_key = nacl.signing.SigningKey.generate()
        self.type = 'nacl.signing.SigningKey'

    def sign(self, message):
        '''
        :param bytes message: message to be signed
        '''
        if isinstance(message, str):
            message = message.encode()
        return self.secret_key.sign(message)


class VerifyKey:
    def __init__(self, verify_key):
        assert isinstance(verify_key, nacl.signing.VerifyKey)
        self.verify_key = verify_key

    @classmethod
    def from_SigningKey(cls, secret_key):
        verify_key = secret_key.secret_key.verify_key
        return VerifyKey(verify_key)

    @classmethod
    def from_hex(cls, verify_key_hex):
        verify_key = nacl.signing.VerifyKey(verify_key_hex, encoder=nacl.encoding.HexEncoder)
        return VerifyKey(verify_key)

    def verify_signature(self, signature, message):
        '''
        :param bytes signature: signature to verify
        :param bytes message: message that was supposedly signed
        '''
        if isinstance(message, str):
            message = message.encode()
        return self.verify_key.verify(signature) == message

    def to_hex(self):
        return self.verify_key.encode(encoder=nacl.encoding.HexEncoder)