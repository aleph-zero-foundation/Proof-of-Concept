#!/usr/bin/env python3
from aleph.crypto.keys import SigningKey, VerifyKey

def save_keys(keys, filename):
    with open(filename, "w") as the_file:
        for key in keys:
            the_file.write(key.decode('utf8')+"\n")

def save_priv_keys(priv_keys, i):
    save_keys(priv_keys, "key" + str(i) + ".secret")

def save_pub_keys(pub_keys):
    save_keys(pub_keys, "keys.public")

if __name__ == '__main__':
    import sys
    assert(len(sys.argv) == 3)
    n_machines = int(sys.argv[1])
    processes_per_machine = int(sys.argv[2])
    pub_keys = []
    for i in range(n_machines):
        priv_keys = []
        for _ in range(processes_per_machine):
            priv_key = SigningKey()
            pub_keys.append(VerifyKey.from_SigningKey(priv_key).to_hex())
            priv_keys.append(priv_key.to_hex())
        save_priv_keys(priv_keys, i)
    save_pub_keys(pub_keys)
