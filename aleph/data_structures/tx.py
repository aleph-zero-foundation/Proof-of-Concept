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

class Tx(object):
    '''A class representing a single transaction, that is an act of sending some number of tokens from one user to another.'''
    '''
    This class stores a transactions issued by some user

    :param str issuer: public key of the issuer of the transaction
    :param str receiver: public key of the receiver of the transaction
    :param int amount: amount to be sent to the receiver
    '''

    __slots__ = ['issuer', 'receiver', 'amount']

    def __init__(self, issuer, receiver, amount):
        self.issuer = issuer
        self.receiver = receiver
        self.amount = amount


    def __getstate__(self):
        return (self.issuer, self.receiver, self.amount)


    def __setstate__(self, state):
        self.issuer, self.receiver, self.amount = state


    def __str__(self):
        tx_string =  'Issuer: ' + str(self.issuer) + '\n'
        tx_string += 'Receiver: ' + str(self.receiver) + '\n'
        tx_string += 'Amount: ' + str(self.amount) + '\n'
        return tx_string

    __repr__ = __str__


    def __eq__(self, other):
        return (isinstance(other, Tx) and self.issuer == other.issuer
                and self.receiver == other.receiver
                and self.amount == other.amount)


    def __hash__(self):
        return hash(str(self))
