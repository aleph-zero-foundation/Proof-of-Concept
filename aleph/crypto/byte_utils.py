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

import hashlib


def extract_bit(bytestring, bit_index):
    '''
    Returns the bit_index'th bit of bytestring.
    It is assumed that bit_index < 8*len(bytestring) since bytestring is an array of bytes, each 8 bits long.

    :param bytes bytestring: bytestring from which the bit is to be extracted
    :param int bit_index: index of bit to be extracted.
    '''
    assert bit_index < 8*len(bytestring), "Attempting to extract a bit with too large of an index."

    byte_index = bit_index // 8
    index_within_byte = bit_index % 8
    mask = (1 << index_within_byte)
    if bytestring[byte_index] & mask:
        return 1
    return 0


def sha3_hash(bytestring):
    '''
    Returns the sha3_256 hash of the bytestring.

    :param bytes bytestring: bytestring of which the hash is calculated.
    '''
    return hashlib.sha3_256(bytestring).digest()


def xor(bytes1, bytes2):
    '''
    Returns a xor of two bytestrings bytes1, bytes2. The length of the result is the max of their lengths.
    If one of them is shorter it is rotated cyclically to obtain a string of matching length.

    :param bytes bytes1: xor's first argument
    :param bytes bytes2: xor's second argument
    '''
    assert bytes1 and bytes2, "An attempt to xor an empty bytestring"
    if len(bytes1) < len(bytes2):
        bytes1, bytes2 = bytes2, bytes1

    result = list(bytes1[:])
    len_bytes2 = len(bytes2)
    for i in range(len(bytes1)):
        result[i] ^= bytes2[i % len_bytes2]

    return bytes(result)
