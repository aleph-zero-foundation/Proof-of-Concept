from fabric import task


@task
def test(conn):
    conn.run('uname -a')


@task
def run_tests(conn):
    with conn.cd('proof-of-concept'):
        conn.run('pytest aleph')


@task
def sync_keys(conn):
    #conn.put()
    pass


@task
def install_dependencies(conn):
    conn.sudo('apt update')

    conn.sudo('apt install -y make flex bison libgmp-dev libmpc-dev libssl-dev')
    conn.sudo('apt install -y python3-dev python3-pip')
    conn.sudo('pip3 install setuptools pytest-xdist')

    conn.run('wget https://crypto.stanford.edu/pbc/files/pbc-0.5.14.tar.gz')
    conn.run('tar -xvf pbc-0.5.14.tar.gz')
    with conn.cd('pbc-0.5.14'):
        conn.run('./configure')
        conn.run('make')
        conn.run('sudo make install')

    conn.run('git clone https://github.com/JHUISI/charm.git')
    with conn.cd('charm'):
        conn.run('./configure.sh')
        conn.run('make')
        conn.run('sudo make install')


@task
def clone_repo(conn):
    conn.run('git clone http://gitlab+deploy-token-38770:usqkQKRbQiVFyKZ2byZw@gitlab.com/alephledger/proof-of-concept.git')
    with conn.cd('proof-of-concept'):
        conn.run('git checkout devel')
        conn.run('sudo pip3 install -e .')

@task
def init(conn):
    sync_keys(conn)
    install_dependencies(conn)
    clone_repo(conn)


@task
def stop_world(conn):
    pass
