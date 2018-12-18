from itertools import cycle

def xor(bytes1, bytes2):
        '''
        Returns a xor of two bytestrings bytes1, bytes2. The length of the result is the max of their lengths.
        If one of them is shorter it is rotated cyclically to obtain a string of matching length.
        '''
        assert len(bytes1) > 0 and len(bytes2) > 0, "An attempt to xor an empty bytestring"
        if len(bytes1) < len(bytes2):
            bytes1, bytes2 = bytes2, bytes1

        return bytes(a ^ b for (a, b) in zip(bytes1, cycle(bytes2)))
