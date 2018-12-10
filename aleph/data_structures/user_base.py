import logging

from aleph.config import *


class User_base:
    '''
    This class is used to store information about user accounts: their balances and last succesful transactions.
    '''


    def __init__(self):
        '''
        '''
        self.user_balance = {}
        self.user_last_transaction_index = {}



    def account_balance(self, user_public_key):
        return user_balance.get(user_public_key, 0)



    def last_transaction(self, user_public_key):
        return user_last_transaction_index.get(user_public_key, -1)



    def check_transaction_correctness(self, tx):
        issuer_balance = self.user_balance.get(tx.issuer, 0)
        issuer_last_transaction = self.user_last_transaction_index.get(tx.issuer, -1)
        return tx.amount >= 0 and issuer_balance >= tx.amount and tx.index == issuer_last_transaction + 1



    def apply_transaction(self, tx):
        issuer_balance = self.user_balance.get(tx.issuer, 0)
        receiver_balance = self.user_balance.get(tx.receiver, 0)
        issuer_last_transaction = self.user_last_transaction_index.get(tx.issuer, -1)

        assert tx.amount >= 0, "The transaction that is about to be input has negative amount!"

        assert issuer_balance >= tx.amount, "The issuer does not have sufficient funds to perform this transaction."

        assert tx.index == issuer_last_transaction + 1, "The index of the transaction is not equal to the previous plus one."

        issuer_balance -= tx.amount
        receiver_balance += tx.amount

        user_balance[tx.issuer] = issuer_balance
        user_balance[tx.receiver] = receiver_balance
        user_last_transaction_index[tx.issuer] = tx.index
