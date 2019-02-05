from aleph.const import LOGGER_NAME

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
logger = logging.getLogger(LOGGER_NAME)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('aleph.log', mode='w')
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter(log_format))
logger.addHandler(fh)

import time
# use gmt time for logging
logging.Formatter.converter = time.gmtime
