from itertools import cycle

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
