#! /bin/bash

echo update > setup.log
sudo apt update

echo install from ubuntu repo >> setup.log
sudo apt install -y make flex bison unzip libgmp-dev libmpc-dev libssl-dev
sudo apt install -y python3.7-dev python3-pip

echo install from pip repo >> setup.log
sudo python3.7 -m pip install setuptools pytest-xdist pynacl pynacl networkx numpy matplotlib joblib 

echo install pbc >> setup.log
wget https://crypto.stanford.edu/pbc/files/pbc-0.5.14.tar.gz
tar -xvf pbc-0.5.14.tar.gz
cd pbc-0.5.14
./configure
make
sudo make install
cd

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib
export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/lib

echo install charm >> setup.log
git clone https://github.com/JHUISI/charm.git
cd charm
./configure.sh
make
sudo make install
cd

echo clone repo >> setup.log
user_token=gitlab+deploy-token-38770:usqkQKRbQiVFyKZ2byZw
git clone http://${user_token}@gitlab.com/alephledger/proof-of-concept.git
cd proof-of-concept
git checkout devel
cd

echo done >> setup.log
