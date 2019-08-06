import nacl.signing


class SigningKey:
    '''
    Implements signing (private) key.

    :param string seed_hex: seed used for instantiating this class; if None a key is generated.
    '''

    def __init__(self, seed_hex=None):
        if seed_hex is not None:
            self.secret_key = nacl.signing.SigningKey(seed_hex, encoder=nacl.encoding.HexEncoder)
        else:
            self.secret_key = nacl.signing.SigningKey.generate()

    def sign(self, message):
        '''
        Returns signed message.

        :param bytes message: message to be signed
        '''

        if isinstance(message, str):
            message = message.encode()

        return self.secret_key.sign(message).signature

    def to_hex(self):
        ''' Returns hex representation of the secret. It is used for serialization.  '''

        return nacl.encoding.HexEncoder.encode(self.secret_key._seed)


class VerifyKey:
    ''' Implements verification (public) key.

    :param nacl.signing.VerifyKey verify_key: key used to instantiate this class
    '''

    def __init__(self, verify_key):

        assert isinstance(verify_key, nacl.signing.VerifyKey), 'Wrong class used to instantiation, use nacl.signing.VerifyKey'
        self.verify_key = verify_key

    @staticmethod
    def from_SigningKey(secret_key):
        '''
        A factory generating object of this class from secret_key

        :param SigningKey secret_key: key used to create VerifyKey
        :returns: VerifyKey object
        '''

        return VerifyKey(secret_key.secret_key.verify_key)

    @staticmethod
    def from_hex(verify_key_hex):
        '''
        A vactory generating object of this class from hex representation

        :param string verify_key_hex: hex representation of VerifyKey
        :returns: VerifyKey object
        '''

        return VerifyKey(nacl.signing.VerifyKey(verify_key_hex, encoder=nacl.encoding.HexEncoder))

    def verify_signature(self, signature, message):
        '''
        Verifies signature of the message.

        :param bytes signature: signature to verify
        :param bytes message: message that was supposedly signed
        :returns: True if signature is correct, False otherwise
        '''

        if isinstance(message, str):
            message = message.encode()
        try:
            self.verify_key.verify(message, signature)
        except nacl.exceptions.BadSignatureError:
            return False

        return True

    def to_hex(self):
        '''
        Returns hex representation of this object. It is used for serialization.
        '''

        return self.verify_key.encode(encoder=nacl.encoding.HexEncoder)
