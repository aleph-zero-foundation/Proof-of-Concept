from itertools import cycle
import hashlib


def extract_bit(bytestring, bit_index):
    '''
    Returns the bit_index'th bit of bytestring.
    It is assumed that bit_index < 8*len(bytestring) since bytestring is an array of bytes, each 8 bits long.
    '''
    assert bit_index < 8*len(bytestring), "Attempting to extract a bit with too large of an index."

    byte_index = bit_index // 8
    index_within_byte = bit_index % 8
    mask = (1 << index_within_byte)
    if (bytestring[byte_index] & mask):
        return 1
    else:
        return 0


def sha3_hash(bytestring):
    '''
    Returns the sha3_256 hash of the bytestring.
    '''
    return hashlib.sha3_256(bytestring).digest()


def xor(bytes1, bytes2):
    '''
    Returns a xor of two bytestrings bytes1, bytes2. The length of the result is the max of their lengths.
    If one of them is shorter it is rotated cyclically to obtain a string of matching length.
    '''
    assert len(bytes1) > 0 and len(bytes2) > 0, "An attempt to xor an empty bytestring"
    if len(bytes1) < len(bytes2):
        bytes1, bytes2 = bytes2, bytes1

    s = list(bytes1[:])
    len_bytes2 = len(bytes2)
    for i in range(len(bytes1)):
    	s[i] ^= bytes2[i%len_bytes2]

    return bytes(s)
