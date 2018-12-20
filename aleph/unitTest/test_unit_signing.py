from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.data_structures import Unit
from aleph.process import Process

class TestSignatures():
    def test_one(self):
        sk = SigningKey()
        vk = VerifyKey.from_SigningKey(sk)
        U = Unit(0,[],[])
        msg = U.to_message()
        signature = sk.sign(msg)
        assert vk.verify_signature(signature, msg)

    def test_two(self):
        process_id = 0
        n_processes = 100
        sk = SigningKey()
        vk = VerifyKey.from_SigningKey(sk)
        dummy_keys = [None for _ in range(n_processes)]
        dummy_addresses = [(None, None) for _ in range(n_processes)]
        process = Process(n_processes, process_id, sk, vk, dummy_addresses, dummy_keys, None)
        U = Unit(0,[],[])
        process.sign_unit(U)

        msg = U.to_message()
        assert vk.verify_signature(U.signature, msg)

