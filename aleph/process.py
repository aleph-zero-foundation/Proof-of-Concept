import random
import asyncio

from aleph.data_structures.unit import Unit
from aleph.data_structures.poset import Poset
from aleph.crypto.signatures.keys import PrivateKey, PublicKey
from aleph.network import listener, connecter
from aleph.config import CREATE_FREQ, SYNC_INIT_FREQ




class Process:
    '''This class is the main component of the Aleph protocol.'''


    def __init__(self, n_processes, process_id, secret_key, address_list, public_key_list):
        '''
        :param int n_processes: the committee size
        :param int process_id: the id of the current process
        :param string secret_key: the private key of the current process
        :param list addresses: the list of length n_processes containing addresses (host, port) of all committee members
        :param list public_keys: the list of public keys of all committee members
        '''

        self.n_processes = n_processes
        self.process_id = process_id

        self.secret_key = secret_key
        self.public_key = public_key_list[process_id]

        self.public_key_list = public_key_list
        self.address_list = address_list
        self.host = address_list[process_id][0]
        self.port = address_list[process_id][1]

        self.poset = Poset(self.n_processes, self.secret_key, self.public_key)

        # a bitmap specifying for every process whether he has been detected forking
        self.is_forker = [False for _ in range(self.n_processes)]


    async def create_add(self):
    	while True:
    		new_unit = self.poset.create_unit()
    		if new_unit is not None:
    			assert self.poset.check_compliance(new_unit), "A unit created by our process is not passing the compliance test!"
    			self.poset.add_unit(new_unit)
    			await asyncio.sleep(CREATE_FREQ)



    async def _run_tasks(self):
    	tasks = []
    	tasks.append(asyncio.create_task(self.create_add()))
    	# TODO: add listener and connected tasks

    	await asyncio.gather(*tasks)


    def run(self):
    	asyncio.run(self._run_tasks())

