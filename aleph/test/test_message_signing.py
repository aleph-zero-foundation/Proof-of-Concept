import random
import string

from aleph.crypto import SigningKey, VerifyKey

def test_true():
    sk = SigningKey()
    vk = VerifyKey.from_SigningKey(sk)
    for _ in range(10):
        n = random.randint(50,200)
        msg = ''.join(random.choices(string.printable, k=n))
        if random.randint(0,1):
            msg = msg.encode()
        sign = sk.sign(msg)
        assert vk.verify_signature(sign, msg)


def test_false():
    sk = SigningKey()
    vk = VerifyKey.from_SigningKey(sk)
    for _ in range(10):
        n = random.randint(50,200)
        msg = ''.join(random.choices(string.printable, k=n))
        if random.randint(0,1):
            msg = msg.encode()
        sign = sk.sign(msg)
        k = random.randint(0, n-1)
        msg = msg[:k] + msg[k+1:]
        assert not vk.verify_signature(sign, msg)
