'''Routines called by fab. Assumes that all are called from */experiments/aws.'''

from fabric import task


#======================================================================================
#                                    installation
#======================================================================================

@task
def install_dependencies(conn):
    '''Installs dependencies via fabric. Deprecated since it blocks till finished.'''

    conn.sudo('apt update')

    # install dependencies for building pbc and charm
    conn.sudo('apt install -y make flex bison unzip libgmp-dev libmpc-dev libssl-dev')
    conn.sudo('apt install -y python3.7-dev python3-pip')

    conn.sudo('python3.7 -m pip install setuptools pytest-xdist pynacl')

    # download and install pbc
    conn.run('wget https://crypto.stanford.edu/pbc/files/pbc-0.5.14.tar.gz')
    conn.run('tar -xvf pbc-0.5.14.tar.gz')
    with conn.cd('pbc-0.5.14'):
        conn.run('./configure')
        conn.run('make')
        conn.run('sudo make install')

    # exports paths were pbc is installed
    conn.run('export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib')
    conn.run('export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/lib')

    # download and install charm
    conn.run('git clone https://github.com/JHUISI/charm.git')
    with conn.cd('charm'):
        conn.run('./configure.sh')
        conn.run('make')
        conn.run('sudo make install')


@task
def clone_repo(conn):
    '''Clones main repo, checkouts to devel, and installs it via pip.'''

    # delete current repo
    conn.run('rm -rf proof-of-concept')
    # clone using deployment token
    user_token = 'gitlab+deploy-token-38770:usqkQKRbQiVFyKZ2byZw'
    conn.run(f'git clone http://{user_token}@gitlab.com/alephledger/proof-of-concept.git')
    # install via pip
    with conn.cd('proof-of-concept'):
        conn.run('git checkout devel')
        # python 3.7 is established in virtual env, hence we need to activate it in every conn.run
        conn.run('source /home/ubuntu/p37/bin/activate && pip install .', hide='out')


@task
def sync_keys(conn):
    '''Syncs signing_keys - public keys for the committee.'''

    conn.put('signing_keys', 'proof-of-concept/experiments')


@task
def sync_addresses(conn):
    '''Syncs ip addresses of the committee members.'''

    conn.run(f'echo {conn.host} > proof-of-concept/experiments/my_ip')
    conn.put('hosts', 'proof-of-concept/experiments/')


@task
def init(conn):
    '''Dispatches all tasks preparing the environment, i.e.
       installs dependencies, clones repo, and syncs keys and hosts' adresses. '''

    print('installing dependencies')
    install_dependencies(conn)
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

    # rename main repo
    conn.run('mv proof-of-concept proof-of-concept.old')
    # send it upstream
    conn.put('../../../poc.zip', '.')
    # unpack
    conn.run('unzip -q poc.zip')
    # install new version
    install_repo(conn)


@task
def delete_testing_repo(conn):
    ''' Restore the main repo.'''

    conn.sudo('rm -rf proof-of-concept')
    conn.run('mv proof-of-concept.old proof-of-concept')


@task
def resync_local_repo(conn):
    ''' Resyncs current version of a local repo with a host.'''

    conn.run('rm -rf proof-of-concept')
    # send it upstream
    conn.put('../../../poc.zip', '.')
    # unpack
    conn.run('unzip -q poc.zip')
    # install
    install_repo(conn)
    # resend hosts and keys
    sync_files(conn)


@task
def send_file_simple(conn):
    '''Sends current version of the simple test. It does not need installing as it is called diractly.'''

    conn.put('../simple_ec2_test.py', 'proof-of-concept/experiments/')

#======================================================================================
#                                   run experiments
#======================================================================================

@task
def simple_ec2_test(conn):
    ''' Sends current version of experiment and runs it.'''

    conn.put('../simple_ec2_test.py', 'proof-of-concept/experiments/')
    with conn.cd('proof-of-concept/experiments'):
        # export env var needed for pbc, activate venv, cross fingers, and run the experiment
        cmd = 'python simple_ec2_test.py -i hosts -k signing_keys -l 10 -b 65536 -u -1000'
        conn.run('export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib &&'
                 'source /home/ubuntu/p37/bin/activate &&'
                 f'dtach -n `mktemp -u /tmp/dtach.XXXX` {cmd}')

@task
def get_logs(conn):
    ''' Retrieves aleph.log from the server.'''

    ip = conn.host.replace('.', '-')
    conn.get('proof-of-concept/experiments/aleph.log', f'../results/{ip}-aleph.log')


@task
def stop_world(conn):
    ''' Kills the committee member.'''

    # it is safe as python refers to venv version
    conn.run('killall python')


@task
def test(conn):
    ''' Always changing task for experimenting with fab.'''

    #print(type(conn.host))
    conn.run('source p37/bin/activate && pip install proof-of-concept/', hide='out')


#======================================================================================
#                                   ?
#======================================================================================


@task
def run_unit_tests(conn):
    ''' Exactly as the name says.'''

    with conn.cd('proof-of-concept'):
        conn.run('pytest aleph')


@task
def sync_files(conn):
    ''' Syncs files needed for running a process.'''

    # send files: hosts, signing_keys, setup.sh, set_env.sh, light_nodes_public_keys
    conn.run(f'echo {conn.host} > proof-of-concept/experiments/my_ip')
    conn.put('hosts', 'proof-of-concept/experiments/')
    conn.put('signing_keys', 'proof-of-concept/experiments/')
    conn.put('light_nodes_public_keys', 'proof-of-concept/experiments/')


@task
def inst_dep(conn):
    ''' Install dependencies in a nonblocking way.'''

    conn.put('setup.sh', '.')
    conn.put('set_env.sh', '.')
    conn.sudo('apt update', hide='out')
    conn.sudo('apt install dtach', hide='out')
    conn.run('dtach -n `mktemp -u /tmp/dtach.XXXX` bash setup.sh', hide='out')


@task
def inst_dep_completed(conn):
    ''' Check if installation completed.'''

    result = conn.run('tail -1 setup.log')
    return result.stdout.strip()


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
