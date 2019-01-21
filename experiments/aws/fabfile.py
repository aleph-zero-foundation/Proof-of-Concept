from fabric import task


@task
def install_dependencies(conn):
    conn.sudo('apt update')

    conn.sudo('apt install -y make flex bison unzip libgmp-dev libmpc-dev libssl-dev')
    conn.sudo('apt install -y python3.7-dev python3-pip')

    conn.sudo('python3.7 -m pip install setuptools pytest-xdist pynacl')

    conn.run('wget https://crypto.stanford.edu/pbc/files/pbc-0.5.14.tar.gz')
    conn.run('tar -xvf pbc-0.5.14.tar.gz')
    with conn.cd('pbc-0.5.14'):
        conn.run('./configure')
        conn.run('make')
        conn.run('sudo make install')

    conn.run('export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib')
    conn.run('export LIBRARY_PATH=$LIBRARY_PATH:/usr/local/lib')

    conn.run('git clone https://github.com/JHUISI/charm.git')
    with conn.cd('charm'):
        conn.run('./configure.sh')
        conn.run('make')
        conn.run('sudo make install')


@task
def clone_repo(conn):
    user_token = 'gitlab+deploy-token-38770:usqkQKRbQiVFyKZ2byZw'
    conn.run(f'git clone http://{user_token}@gitlab.com/alephledger/proof-of-concept.git')
    with conn.cd('proof-of-concept'):
        conn.run('git checkout devel')
        conn.run('sudo python3.7 -m pip install -e .')


@task
def sync_keys(conn):
    with conn.cd('proof-of-concept/experiments/aws'):
        conn.put('signing_keys', '.')


@task
def sync_hosts(conn):
    with conn.cd('proof-of-concept/experiments/aws'):
        conn.put('hosts', '.')


@task
def init(conn):
    print('installing dependencies')
    install_dependencies(conn)
    print('cloning the repo')
    clone_repo(conn)
    print('syncing signing keys')
    sync_keys(conn)
    print('syncing hosts file')
    sync_hosts(conn)
    print('init complete')


@task
def send_testing_repo(conn):
    # rename main repo temporarily
    conn.run('mv proof-of-concept proof-of-concept.old')
    # pack current version
    conn.local('zip -rq poc.zip ../../../proof-of-concept')
    # send it upstream
    conn.put('poc.zip', '.')
    # unpack
    conn.run('unzip -q poc.zip')


@task
def delete_testing_repo(conn):
    conn.sudo('rm -rf proof-of-concept')
    conn.run('mv proof-of-concept.old proof-of-concept')


@task
def run_simple_ec2_test(conn):
    conn.run('env PYTHONPATH=.:/home/ubuntu/proof-of-concept echo $PYTHONPATH')
    with conn.cd('proof-of-concept/experiments'):
        conn.run('env PYTHONPATH=.:/home/ubuntu/proof-of-concept python3.7 simple_ec2_test.py -h hosts -s signing_keys')

@task
def run_processes(conn):
    with conn.cd('proof-of-concept/experiments'):
        conn.run('python simple_ec2_test -s aws/signing_keys -h aws/hosts')

@task
def stop_world(conn):
    pass


@task
def test(conn):
    conn.run('ls', hide='out')


@task
def run_tests(conn):
    with conn.cd('proof-of-concept'):
        conn.run('pytest aleph')


@task
def sync_files(conn):
    # send files: hosts, signing_keys, setup.sh, set_env.sh
    with conn.cd('proof-of-concept/experiments/aws'):
        conn.put('hosts', '.')
        conn.put('signing_keys', '.')
        conn.put('setup.sh', '.')
        conn.put('set_env.sh', '.')

@task
def inst_dep(conn):
    conn.sudo('apt update', hide='out')
    conn.sudo('apt install dtach', hide='out')
    conn.run('dtach -n `mktemp -u /tmp/dtach.XXXX` bash setup.sh', hide='out')


@task
def inst_dep_completed(conn):
    result = conn.run('tail -1 setup.log')
    return result.stdout.strip()


@task
def date(conn):
    conn.run('date')

