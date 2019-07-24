'''Routines called by fab. Assumes that all are called from */experiments/aws.'''

from fabric import task


#======================================================================================
#                                    installation
#======================================================================================

@task
def sync_files(conn):
    ''' Syncs files needed for running a process.'''

    # send files: addresses, signing_keys, setup.sh, set_env.sh, light_nodes_public_keys
    conn.run(f'echo {conn.host} > proof-of-concept/aleph/my_ip')
    conn.put('ip_addresses', 'proof-of-concept/aleph/')
    conn.put('signing_keys', 'proof-of-concept/aleph/')
    conn.put('light_nodes_public_keys', 'proof-of-concept/aleph/')


@task
def inst_dep(conn):
    ''' Install dependencies in a nonblocking way.'''

    conn.put('setup.sh', '.')
    conn.put('set_env.sh', '.')
    conn.sudo('apt update', hide='both')
    conn.sudo('apt install dtach', hide='both')
    conn.run('dtach -n `mktemp -u /tmp/dtach.XXXX` bash setup.sh', hide='both')


@task
def inst_dep_completed(conn):
    ''' Check if installation completed.'''

    result = conn.run('tail -1 setup.log')
    return result.stdout.strip()


@task
def clone_repo(conn):
    '''Clones main repo, checkouts to devel, and installs it via pip.'''

    # delete current repo
    conn.run('rm -rf proof-of-concept')
    # clone using deployment token
    user_token = 'gitlab+deploy-token-38770:usqkQKRbQiVFyKZ2byZw'
    conn.run(f'git clone http://{user_token}@gitlab.com/alephledger/proof-of-concept.git')
    # checkout to devel
    with conn.cd('proof-of-concept'):
        conn.run('git checkout devel')

    # install current version
    install_repo(conn)


@task
def sync_keys(conn):
    '''Syncs signing_keys - public keys for the committee.'''

    conn.put('signing_keys', 'proof-of-concept/aleph')


@task
def sync_addresses(conn):
    '''Syncs ip addresses of the committee members.'''

    conn.put('ip_addresses', 'proof-of-concept/aleph/')


@task
def init(conn):
    '''Dispatches all tasks preparing the environment, i.e.
       installs dependencies, clones repo, and syncs keys and hosts' adresses. '''

    print('installing dependencies')
    inst_dep(conn)
    print('cloning the repo')
    clone_repo(conn)
    print('syncing signing keys')
    sync_keys(conn)
    print('syncing addresses')
    sync_addresses(conn)
    print('init complete')


@task
def install_repo(conn):
    '''Installs the repo via pip in the virtual env.'''

    conn.run('source p37/bin/activate && pip install proof-of-concept/', hide='out')

#======================================================================================
#                                   syncing local version
#======================================================================================

@task
def zip_repo(conn):
    ''' Zips local version of the repo for sending it to a host.'''

    # clears __pycache__
    conn.local("find ../../../proof-of-concept -name '*.pyc' -delete")
    # remove logs
    conn.local("find ../../../proof-of-concept -name '*.log' -delete")
    # remove arxives
    conn.local("find ../../../proof-of-concept -name '*.zip' -delete")

    with conn.cd('../../..'):
        conn.local('zip -rq poc.zip proof-of-concept -x "*/.*"')


@task
def send_testing_repo(conn):
    ''' Sends zipped local version of the repo to a host.'''

    # remove current version
    conn.run('rm -rf proof-of-concept')
    # send local repo upstream
    conn.put('../../../poc.zip', '.')
    # unpack
    conn.run('unzip -q poc.zip')
    # install new version
    install_repo(conn)


@task
def send_file_simple(conn):
    '''Sends current version of the simple test. It does not need installing as it is called diractly.'''

    conn.put('../simple_ec2_test.py', 'proof-of-concept/experiments/')


@task
def send_file_main(conn):
    '''Sends current version of the main. It does not need installing as it is called diractly.'''

    conn.put('../../aleph/main.py', 'proof-of-concept/aleph/')

#======================================================================================
#                                   run experiments
#======================================================================================

@task
def simple_ec2_test(conn):
    ''' Sends current version of experiment and runs it.'''

    conn.put('../simple_ec2_test.py', 'proof-of-concept/experiments/')
    with conn.cd('proof-of-concept/experiments'):
        cmd = 'python simple_ec2_test.py -i addresses -k signing_keys -l 10 -b 1000000 -u 1000'
        # export env var needed for pbc, activate venv, cross fingers, and run the experiment
        conn.run('export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib &&'
                 'source /home/ubuntu/p37/bin/activate &&'
                 f'dtach -n `mktemp -u /tmp/dtach.XXXX` {cmd}')


@task
def send_params(conn):
    ''' Sends parameters for expermients'''
    conn.put('../../aleph/main.py', 'proof-of-concept/aleph/')
    conn.put('const.py', 'proof-of-concept/aleph')
    install_repo(conn)

@task
def run_protocol(conn):
    ''' Runs the protocol.'''

    with conn.cd('proof-of-concept/aleph'):
        # export env var needed for pbc, activate venv, cross fingers, and run the protocol
        cmd = 'python main.py'
        conn.run('export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib &&'
                 'source /home/ubuntu/p37/bin/activate &&'
                 'ulimit -n 2048 &&'
                 f'dtach -n `mktemp -u /tmp/dtach.XXXX` {cmd}')

@task
def get_logs(conn):
    ''' Retrieves aleph.log from the server.'''

    ip = conn.host.replace('.', '-')

    with conn.cd('proof-of-concept/aleph/'):
        conn.run(f'zip -q {ip}-aleph.log.zip aleph.log')
    conn.get(f'proof-of-concept/aleph/{ip}-aleph.log.zip', f'../results/{ip}-aleph.log.zip')

@task
def get_dag(conn):
    ''' Retrieves poset.dag from the server.'''

    conn.get(f'proof-of-concept/aleph/poset.dag', f'../poset.dag')

@task
def stop_world(conn):
    ''' Kills the committee member.'''

    # it is safe as python refers to venv version
    conn.run('killall python')


@task
def test(conn):
    ''' Always changing task for experimenting with fab.'''

    conn.open()


@task
def run_unit_tests(conn):
    ''' Exactly as the name says.'''

    with conn.cd('proof-of-concept'):
        conn.run('pytest aleph')


#======================================================================================
#                                   ?
#======================================================================================


#======================================================================================
#                                   new
#======================================================================================

@task
def experiment_started(conn):
    pass


@task
def experiment_stopped(conn):
    pass

@task
def latency(conn):
    pass
