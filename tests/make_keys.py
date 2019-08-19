'''
    This is a Proof-of-Concept implementation of Aleph Zero consensus protocol.
    Copyright (C) 2019 Aleph Zero Team
    
    This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
    This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
    
    You should have received a copy of the GNU General Public License
    along with this program. If not, see <http://www.gnu.org/licenses/>.
'''

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
    assert len(sys.argv) == 3
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
