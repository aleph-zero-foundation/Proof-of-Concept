#! /bin/bash

set -e 

echo update > setup.log
sudo apt update

echo install from ubuntu repo >> setup.log
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y make flex bison zip unzip virtualenv libgmp-dev libmpc-dev libssl1.0-dev
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y python3.7-dev python3-pip

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

echo compile charm >> setup.log
git clone https://github.com/JHUISI/charm.git
cd charm
./configure.sh
make
cd

echo setting up env >> setup.log
virtualenv --python=python3.7 p37
source p37/bin/activate

echo install charm >> setup.log
cd charm
pip install .
cd

echo install from pip repo >> setup.log
pip install setuptools pytest-xdist pynacl networkx numpy matplotlib joblib 

echo done >> setup.log
