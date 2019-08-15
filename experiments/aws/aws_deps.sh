#! /bin/bash

set -e

print () {
    echo -e "\033[0;32m$@\033[0m"
}

print installing parallel
sudo apt-get install -y parallel

print enterintg virtenv p37

source $HOME/p37/bin/activate

print installing fabric, boto3, ipython
pip install fabric boto3 ipython
