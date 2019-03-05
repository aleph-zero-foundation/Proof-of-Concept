from aleph.crypto.keys import SigningKey, VerifyKey
from aleph.data_structures import Unit
from aleph.process import Process


def test_signing():
    '''
    Tests whether a correct signature of a Unit.bytestring() is positively verified.
    '''
    sk = SigningKey()
    vk = VerifyKey.from_SigningKey(sk)
    U = Unit(0,[],[])
    msg = U.bytestring()
    signature = sk.sign(msg)
    assert vk.verify_signature(signature, msg)

def test_process_signing():
    '''
    Tests whether a process correctly signs a Unit.
    '''
    process_id = 0
    n_processes = 100
    sk = SigningKey()
    vk = VerifyKey.from_SigningKey(sk)
    dummy_keys = [VerifyKey.from_SigningKey(SigningKey()) for _ in range(n_processes)]
    dummy_keys[0] = vk
    dummy_addresses = [(None, None) for _ in range(n_processes)]
    process = Process(n_processes, process_id, sk, vk, dummy_addresses, dummy_keys, None)
    U = Unit(0,[],[])
    process.sign_unit(U)

    msg = U.bytestring()
    assert vk.verify_signature(U.signature, msg)

