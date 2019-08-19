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

import logging


class UserDB:
    '''
    This class is used to store information about user accounts: their balances and last succesful transactions.
    '''


    def __init__(self, initial_balances_and_indices = []):
        '''
        Creates a user data base that contains user balances (by public key) and their last validated transaction.
        :param list initial_balances_and_indices: a list of triples consisting of a user's public key, their intial balance and the index of the last transaction performed by them
        '''
        self.user_balance = {}
        self.user_last_transaction_index = {}
        for public_key, balance, index in initial_balances_and_indices:
            self.user_balance[public_key] = balance
            self.user_last_transaction_index[public_key] = index


    def account_balance(self, user_public_key):
        '''
        Get the balance of the given user.
        :param str user_public_key: the public key of the user
        '''
        return self.user_balance.get(user_public_key, 0)


    def last_transaction(self, user_public_key):
        '''
        Get the index of the last transaction issued by the given user.
        :param str user_public_key: the public key of the user
        '''
        return self.user_last_transaction_index.get(user_public_key, -1)


    def check_transaction_correctness(self, tx):
        '''
        Check the correctness of a given transaction.
        :param Tx tx: the transaction to check
        :returns: True if the transaction has index one higher than the last transaction made by its issuer and the balance allows for the transaction, False otherwise
        '''
        issuer_balance = self.user_balance.get(tx.issuer, 0)
        issuer_last_transaction = self.user_last_transaction_index.get(tx.issuer, -1)
        return tx.amount >= 0 and issuer_balance >= tx.amount and tx.index == issuer_last_transaction + 1


    def apply_transaction(self, tx):
        '''
        Performs the given transaction if it is valid.
        :param Tx tx: the transaction to perform
        '''
        if self.check_transaction_correctness(tx):
            self.user_balance[tx.issuer] -= tx.amount
            self.user_balance[tx.receiver] += tx.amount
            self.user_last_transaction_index[tx.issuer] += 1

