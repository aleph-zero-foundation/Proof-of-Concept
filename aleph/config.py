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

import aleph.const as consts

import charm.toolbox.pairinggroup
PAIRING_GROUP = charm.toolbox.pairinggroup.PairingGroup('MNT224')
# initialize group generator
GENERATOR = PAIRING_GROUP.hash('gengen', charm.toolbox.pairinggroup.G2)
# precompute exponentiation table to speed up computations
GENERATOR.initPP()

import pickle
pickle.DEFAULT_PROTOCOL = 4

import logging
log_format = '[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s [%(filename)s:%(lineno)d]'
logging.basicConfig(filename='other.log',
                    level=logging.DEBUG,
                    format=log_format,
                    filemode='w')
logger = logging.getLogger(consts.LOGGER_NAME)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('aleph.log', mode='w')
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter(log_format))
logger.addHandler(fh)

import time
# use gmt time for logging
logging.Formatter.converter = time.gmtime
