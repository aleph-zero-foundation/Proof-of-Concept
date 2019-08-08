# Aleph Proof-of-Concept

![aleph logo](docs/source/aleph_1920x1080.jpg "Aleph logo")

Aleph is an asynchronous and Byzantine fault tolerant DAG-based consensus protocol aimed at ordering messages (transactions). It has been designed to operate continuously under conditions where there are no bound on message-delivery delay and under the assumption that there is a significant probability of malicious behavior, making it an excellent fit for blockchain-related applications. For more information, check [webpage](https://alephzero.org).

This repository is a proof-of-concept implementation, it is not meant for production deployment. It is released as a reference for the main implementation in Go that will be published in the future. The initial version of the repository was based on an [old paper](https://arxiv.org/abs/1810.05256), while the more recent one relies on a [new paper](https://arxiv.org/abs/1908.05156).

# Results from experiments run on AWS

The following results come from experiments performed on 128 nodes of Amazon Web Services distributed uniformly between 8 different regions on 5 continents.
We tested the performance under various loads of the system:

|load|txps|latency|
|---|---:|---:|
| small  | 72.1  | 5.1s|
| big | 9476.7 | 15.8s |
| heavy | 93419.0 | 20.5s |

For more results, details of the setup, and discussions check our [reports](https://gitlab.com/alephledger/proof-of-concept/tree/master/reports).

# Documentation

[The documentation](https://alephledger.gitlab.io/proof-of-concept) describes only the algorithmic and technical parts of the implementation. For the conceptual understanding of the protocol and the implementation please read the above papers.

# Merge requests

This repository is only an exploratory playground and we are done with it now. No further developments will be made on our side. If you want to play with it, go ahead and fork it!

# Installation

The implementation requires Python 3.7. The following script creates a virtual environment for Python 3.7 in home directory and installs Aleph and its dependencies there. The script was prepared for Ubuntu 18.04.
  - run `bash experiments/aws/setup.sh`
  - enter py37 env `source $HOME/p37/bin/activate`
  - install aleph `pip install -e .`
Remember to activate py37 env and `export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib` in every terminal you play with Aleph.

# Unit tests
To run unit tests please use the following command: `pytest-xdist -n $n_cores aleph`

# Experiments

There are two types of experiments that can be performed:
1. Local: run `python tests/linear_ordering_network.py`
2. Remote using AWS EC2:
  - Create an account on AWS and set up credentials and a default region as described [here](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration).
  - Then, go to `cd experiments/aws` and install additional packages `bash aws_deps.sh`.
  - Run `python shell.py`. This opens a shell with procedures orchestrating experiments. The main procedure is
  `run_protocol(n_processes, regions, restricted, instance_type)` that runs `n_processes` in specified `regions`, where some of the regions can be `restricted`, and uses EC2 machines of `instance_type`.
  - The most basic experiment can be run with `run_protocol(8, badger_regions(), {}, 't2.micro')`. It spawns 8 machines in 8 different regions on 5 continents. As of time of writing, AWS EC2 was providing users with a limited time of free usage of machines of type `t2.micro` and some quota for free storage and data transfer, so such an experiment can be conducted free of charge.
  - The parameters of the protocol are defined in the file `const.py`.
  - To check whether an experiment has finished, use the procedure `reached_max_level` that returns the number of instances that finished their run.
  - After the experiment is finished, the logs containing useful data of the experiment can be downloaded with `get_logs`.

# Analyzing logs
After collecting the logs, the performance can be analyzed as follows:
1. A single log with data on instance labeled with pid
    `python aleph/log_analyzer/run_analyzer.py aleph.log pid`
2. All logs
    `python aleph/log_analyzer/run_analyzer.py ALL log-dir [report-dir]`

# License
Aleph Python implementation is released under an LGPL version 3 license. See the `LICENSE.txt` for details.
