from aleph.crypto.signatures.keys import PrivateKey, PublicKey
from aleph.data_structures import Unit, Poset


def test_signing():
    sk = PrivateKey()
    pk = PublicKey(sk)
    U = Unit(0,[],[])
    msg = str([U.creator_id, U.parents, U.txs, U.coinshares]).encode()
    signature = sk.sign(msg)
    assert pk.verify_signature(signature, msg)
