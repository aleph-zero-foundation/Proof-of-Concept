from aleph.crypto.signatures.keys import PrivateKey, PublicKey
from aleph.data_structures import Unit, Poset

class TestSignatures():
    def test_one(self):
        sk = PrivateKey()
        pk = PublicKey(sk)
        U = Unit(0,[],[])
        msg = str([U.creator_id, U.parents, U.txs, U.coinshares]).encode()
        signature = sk.sign(msg)
        assert pk.verify_signature(signature, msg)

    def test_two(self):
        genesis_unit = Unit(None,[],[])
        process_id = 0
        n_processes = 100
        sk = PrivateKey()
        pk = PublicKey(sk)
        poset = Poset(n_processes, process_id, genesis_unit, sk, pk)
        U = Unit(0,[],[])
        poset.sign_unit(U)

        msg = str([U.creator_id, U.parents, U.txs, U.coinshares]).encode()
        assert pk.verify_signature(U.signature, msg)

