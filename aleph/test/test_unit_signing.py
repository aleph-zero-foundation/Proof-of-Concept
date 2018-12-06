from aleph.crypto.signatures.keys import PrivateKey, PublicKey
from aleph.data_structures import Unit
from aleph.process import Process

class TestSignatures():
    def test_one(self):
        sk = PrivateKey()
        pk = PublicKey(sk)
        U = Unit(0,[],[])
        msg = U.to_message()
        signature = sk.sign(msg)
        assert pk.verify_signature(signature, msg)

    def test_two(self):
        process_id = 0
        n_processes = 100
        sk = PrivateKey()
        pk = PublicKey(sk)
        dummy_keys = [None for _ in range(n_processes)]
        dummy_addresses = [(None, None) for _ in range(n_processes)]
        process = Process(n_processes, process_id, sk, pk, dummy_addresses, dummy_keys)
        U = Unit(0,[],[])
        process.sign_unit(U)

        msg = U.to_message()
        assert pk.verify_signature(U.signature, msg)

