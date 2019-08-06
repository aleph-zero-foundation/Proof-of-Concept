# AlephZero PoC

ALEPH IMG 

AlephZero is a consensus protocol that is leaderless, fully asynchronous, and Byzantine fault tolerant. Moreover, it was designed to be run repetitively, so it makes a perfect fit for blockchain applications, ie. where there is no bound on message-delivery delay and some malicious behavior is possible to occur. For more information, check [webpage](https://alephzero.org).

This is a proof-of-concept implementation, so it is not production ready and is released only as a reference for the main implementation in Go that will be opensourced in the future. The initial version of the repository was based on [old paper](https://arxiv.org/abs/1810.05256), while the more recent one on [new paper]().

# Results from experiments run on AWS

The following results come from experiments run on 128 nodes spread uniformly on 8 different regions on 5 different continents.
We tested the performance under various loads of the system:

|load|txps|latency|
|---|---:|---:|
| small  | 72.1  | 5.1s|
| big | 9476.7 | 15.8s |
| large | 93419.0 | 20.5s |

For more results, detailed setups, and a discussion check our [reports](https://gitlab.com/alephledger/proof-of-concept/tree/master/reports).

# Documentation

To understand the implementation, it is required to read the papers. [The documentation](https://alephledger.gitlab.io/proof-of-concept) describes only the algorithmic part of the implementation.

# Merge requests

This repository was only an exploratory playground and we are done with it now. If you want to play with it, go ahead and fork it!

# Installation

The implementation requires Python 3.7. The following instructions are for Ubuntu 18.04.

1. pbc 
`wget https://crypto.stanford.edu/pbc/files/pbc-0.5.14.tar.gz`
`tar -xvf pbc-0.5.14.tar.gz`
`cd pbc-0.5.14`
`./configure ; make ; sudo make install`
`export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/lib`
`export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib`
2. dependencies
`pip install -e .`

# Unit tests
To run unit tests use the following command: `pytest-xdist -n $n_cores aleph`

# Experiments

There are two types of experiments that can be performed:
1. Local
    run `python tests/linear_ordering_network.py`
2. Remote using AWS EC2
    go to 'cd experiments/aws'
    and run 'python shell.py'
    This opens a shell with procedures orchestrating experiments. The main procedure is
    `run_protocol(n_processes, regions, restricted, instance_type)` that runs `n_processes` in specified `regions`, where some of the regions may be `restricted`, and uses EC2 machines of `instance_type`.
    The most basic experiment may be run with `run_protocol(8, badger_regions(), {}, 't2.micro')`. It spawns 8 machines on 5 continents in 8 different regions. As of time of writing, AWS EC2 was providing users with a limited time of free usage of machines of type `t2.micro` and some quota for free storage and data transfer. Therefore, such an experiment may be conducted free of charge.
    The parameters for the protocol are defined in the file `const.py`.
    To check whether an experiment has finished, use the procedure `reached_max_level` that returns the number of instances that finished their run.
    After the experiment is finished, the logs containing useful data of the experiment may be downloaded by using the procedure `get_logs`.

# Analyzing logs
After collecting logs, the performance may be analyzed as follows
1. A single log with data on instance labeled with pid
    `python aleph/log_analyzer/run_analyzer.py aleph.log pid`
2. All logs
    `python aleph/log_analyzer/run_analyzer.py ALL log-dif [report-dir]`

# License
This is released under LGPL license. See LICENSE.txt.
