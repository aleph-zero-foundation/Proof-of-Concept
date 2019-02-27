'''This is a shell for orchestrating experiments on AWS EC 2'''
import configparser
import os

from fabric import Connection
from functools import partial
from subprocess import call, check_output
from time import sleep, time
from joblib import Parallel, delayed

import boto3
import numpy as np
import zipfile

from aleph.crypto.keys import SigningKey, VerifyKey
import aleph.const as consts

from fabfile import zip_repo
from utils import image_id_in_region, default_region_name, init_key_pair, security_group_id_by_region, available_regions, badger_regions, generate_signing_keys, n_processes_per_regions, eu_regions

N_JOBS = 8

#======================================================================================
#                              routines for ips
#======================================================================================

def run_task_for_ip(task='test', ip_list=[], parallel=False, output=False):
    '''
    Runs a task from fabfile.py on all instances in a given region.
    :param string task: name of a task defined in fabfile.py
    :param list ip_list: list of ips of hosts
    :param bool parallel: indicates whether task should be dispatched in parallel
    :param bool output: indicates whether output of task is needed
    '''

    print(f'running task {task} in {ip_list}')

    if parallel:
        hosts = " ".join(["ubuntu@"+ip for ip in ip_list])
        cmd = 'parallel fab -i key_pairs/aleph.pem -H {} '+task+' ::: '+hosts
    else:
        hosts = ",".join(["ubuntu@"+ip for ip in ip_list])
        cmd = f'fab -i key_pairs/aleph.pem -H {hosts} {task}'

    try:
        if output:
            return check_output(cmd.split())
        return call(cmd.split())
    except Exception as e:
        print('paramiko troubles')

#======================================================================================
#                              routines for some region
#======================================================================================

def latency_in_region(region_name):
    if region_name == default_region_name():
        region_name = default_region_name()

    print('finding latency', region_name)

    ip_list = instances_ip_in_region(region_name)
    assert ip_list, 'there are no instances running!'

    reps = 10
    cmd = f'parallel nping -q -c {reps} -p 22 ::: ' + ' '.join(ip_list)
    output = check_output(cmd.split()).decode()
    lines = output.split('\n')
    times = []
    for i in range(len(lines)//5):  # equivalent to range(len(ip_list))
        times_ = lines[5*i+2].split('|')
        times_ = [t.split()[2][:-2] for t in times_]
        times.append([float(t.strip()) for t in times_])

    latency = [f'{round(t, 2)}ms' for t in np.mean(times, 0)]
    latency = dict(zip(['max', 'min', 'avg'], latency))

    return latency


def launch_new_instances_in_region(n_processes=1, region_name=default_region_name(), instance_type='t2.micro'):
    '''Launches n_processes in a given region.'''

    print('launching instances in', region_name)

    key_name = 'aleph'
    init_key_pair(region_name, key_name)

    security_group_name = 'aleph'
    security_group_id = security_group_id_by_region(region_name, security_group_name)

    image_id = image_id_in_region(region_name)

    ec2 = boto3.resource('ec2', region_name)
    instances = ec2.create_instances(ImageId=image_id,
                                 MinCount=n_processes, MaxCount=n_processes,
                                 InstanceType=instance_type,
                                 BlockDeviceMappings=[ {
                                     'DeviceName': '/dev/xvda',
                                     'Ebs': {
                                         'DeleteOnTermination': True,
                                         'VolumeSize': 8,
                                         'VolumeType': 'gp2'
                                     },
                                 }, ],
                                 KeyName=key_name,
                                 Monitoring={ 'Enabled': False },
                                 SecurityGroupIds = [security_group_id])

    return instances


def all_instances_in_region(region_name=default_region_name(), states=['running', 'pending']):
    '''Returns all running or pending instances in a given region.'''

    ec2 = boto3.resource('ec2', region_name)
    instances = []
    print(region_name, 'collecting instances')
    for instance in ec2.instances.all():
        if instance.state['Name'] in states:
            instances.append(instance)

    return instances


def terminate_instances_in_region(region_name=default_region_name()):
    '''Terminates all running instances in a given regions.'''

    print(region_name, 'terminating instances')
    for instance in all_instances_in_region(region_name):
        instance.terminate()


def instances_ip_in_region(region_name=default_region_name()):
    '''Returns ips of all running or pending instances in a given region.'''

    ips = []

    for instance in all_instances_in_region(region_name):
        ips.append(instance.public_ip_address)

    return ips


def instances_state_in_region(region_name=default_region_name()):
    '''Returns states of all instances in a given regions.'''

    print(region_name, 'collecting instances states')
    states = []
    possible_states = ['running', 'pending', 'shutting-down', 'terminated']
    for instance in all_instances_in_region(region_name, possible_states):
        states.append(instance.state['Name'])

    return states


def run_task_in_region(task='test', region_name=default_region_name(), parallel=False, output=False):
    '''
    Runs a task from fabfile.py on all instances in a given region.
    :param string task: name of a task defined in fabfile.py
    :param string region_name: region from which instances are picked
    :param bool parallel: indicates whether task should be dispatched in parallel
    :param bool output: indicates whether output of task is needed
    '''

    print(f'running task {task} in {region_name}')

    ip_list = instances_ip_in_region(region_name)
    if parallel:
        hosts = " ".join(["ubuntu@"+ip for ip in ip_list])
        cmd = 'parallel fab -i key_pairs/aleph.pem -H {} '+task+' ::: '+hosts
    else:
        hosts = ",".join(["ubuntu@"+ip for ip in ip_list])
        cmd = f'fab -i key_pairs/aleph.pem -H {hosts} {task}'

    try:
        if output:
            return check_output(cmd.split())
        return call(cmd.split())
    except Exception as e:
        print('paramiko troubles')


def run_cmd_in_region(cmd='tail -f proof-of-concept/experiments/aleph.log', region_name=default_region_name(), output=False):
    '''
    Runs a shell command cmd on all instances in a given region.
    :param string cmd: a shell command that is run on instances
    :param string region_name: region from which instances are picked
    :param bool output: indicates whether output of cmd is needed
    '''

    print(f'running command {cmd} in {region_name}')

    ip_list = instances_ip_in_region(region_name)
    results = []
    for ip in ip_list:
        cmd_ = f'ssh -o "StrictHostKeyChecking no" -q -i key_pairs/aleph.pem ubuntu@{ip} -t "{cmd}"'
        if output:
            results.append(check_output(cmd_, shell=True))
        else:
            results.append(call(cmd_, shell=True))

    return results


def wait_in_region(target_state, region_name):
    '''Waits until all machines in a given region reach a given state.'''

    if region_name == default_region_name():
        region_name = default_region_name()

    print('waiting in', region_name)

    instances = all_instances_in_region(region_name)
    if target_state == 'running':
        for i in instances: i.wait_until_running()
    if target_state == 'terminated':
        for i in instances: i.wait_until_terminated()
    if target_state == 'ssh ready':
        ids = [instance.id for instance in instances]
        initializing = True
        while initializing:
            responses = boto3.client('ec2', region_name).describe_instance_status(InstanceIds=ids)
            statuses = responses['InstanceStatuses']
            all_initialized = True
            if statuses:
                for status in statuses:
                    if status['InstanceStatus']['Status'] != 'ok' or status['SystemStatus']['Status'] != 'ok':
                        all_initialized = False
            else:
                all_initialized = False

            if all_initialized:
                initializing = False
            else:
                print('.', end='')
                import sys
                sys.stdout.flush()
                sleep(5)
        print()


def installation_finished_in_region(region_name):
    '''Checks if installation has finished on all instances in a given region.'''

    results = []
    cmd = "tail -1 setup.log"
    results = run_cmd_in_region(cmd, region_name, output=True)
    for result in results:
        if len(result) < 4 or result[:4] != b'done':
            return False

    print(f'installation in {region_name} finished')
    return True

#======================================================================================
#                              routines for all regions
#======================================================================================

def exec_for_regions(func, regions='all', parallel=True):
    '''A helper function for running routines in all regions.'''

    if regions == 'all':
        regions = available_regions()
    if regions == 'badger regions':
        regions = badger_regions()

    results = []
    if parallel:
        try:
            results = Parallel(n_jobs=N_JOBS)(delayed(func)(region_name) for region_name in regions)
        except Exception as e:
            print('error during collecting results', type(e), e)
    else:
        for region_name in regions:
            results.append(func(region_name))

    if results and isinstance(results[0], list):
        return [res for res_list in results for res in res_list]

    return results


def launch_new_instances(nppr, instance_type='t2.micro'):
    '''
    Launches n_processes_per_region in ever region from given regions.
    :param dict nppr: dict region_name --> n_processes_per_region
    '''

    regions = nppr.keys()

    failed = []
    print('launching instances')
    for region_name in regions:
        print(region_name, '', end='')
        instances = launch_new_instances_in_region(nppr[region_name], region_name, instance_type)
        if not instances:
            failed.append(region_name)

    tries = 5
    while failed and tries:
        tries -= 1
        sleep(5)
        print('there were problems in launching instances in regions', *failed, 'retrying')
        for region_name in failed.copy():
            print(region_name, '', end='')
            instances = launch_new_instances_in_region(nppr[region_name], region_name, instance_type)
            if instances:
                failed.remove(region_name)

    if failed:
        print('reporting complete failure in regions', failed)


def terminate_instances(regions='all', parallel=True):
    '''Terminates all instances in ever region from given regions.'''

    return exec_for_regions(terminate_instances_in_region, regions, parallel)


def all_instances(regions='all', states=['running','pending'], parallel=True):
    '''Returns all running or pending instances from given regions.'''

    return exec_for_regions(partial(all_instances_in_region, states=states), regions, parallel)


def instances_ip(regions='all', parallel=True):
    '''Returns ip addresses of all running or pending instances from given regions.'''

    return exec_for_regions(instances_ip_in_region, regions, parallel)


def instances_state(regions='all', parallel=True):
    '''Returns states of all instances in given regions.'''

    return exec_for_regions(instances_state_in_region, regions, parallel)


def run_task(task='test', regions='all', parallel=True, output=False):
    '''
    Runs a task from fabfile.py on all instances in all given regions.
    :param string task: name of a task defined in fabfile.py
    :param list regions: collections of regions in which the tast should be performed
    :param bool parallel: indicates whether task should be dispatched in parallel
    :param bool output: indicates whether output of task is needed
    '''

    return exec_for_regions(partial(run_task_in_region, task, parallel=parallel, output=output), regions, parallel)


def run_cmd(cmd='ls', regions='all', output=False, parallel=True):
    '''
    Runs a shell command cmd on all instances in all given regions.
    :param string cmd: a shell command that is run on instances
    :param list regions: collections of regions in which the tast should be performed
    :param bool parallel: indicates whether task should be dispatched in parallel
    :param bool output: indicates whether output of task is needed
    '''

    return exec_for_regions(partial(run_cmd_in_region, cmd, output=output), regions, parallel)


def wait(target_state, regions='all'):
    '''Waits until all machines in all given regions reach a given state.'''

    exec_for_regions(partial(wait_in_region, target_state), regions)


def wait_install(regions='all'):
    '''Waits till installation finishes in all given regions.'''

    if regions == 'all':
        regions = available_regions()
    if regions == 'badger regions':
        regions = badger_regions()

    wait_for_regions = regions.copy()
    while wait_for_regions:
        results = Parallel(n_jobs=N_JOBS)(delayed(installation_finished_in_region)(r) for r in wait_for_regions)

        wait_for_regions = [r for i,r in enumerate(wait_for_regions) if not results[i]]
        sleep(10)


#======================================================================================
#                               aggregates
#======================================================================================

def run_protocol(n_processes, regions, restricted, instance_type):
    '''Runs the protocol.'''

    start = time()
    parallel = n_processes > 1
    if regions == 'badger_regions':
        regions = badger_regions()
    if regions == 'all':
        regions = available_regions()

    # note: there are only 5 t2.micro machines in 'sa-east-1', 'ap-southeast-2' each
    print('launching machines')
    nhpr = n_processes_per_regions(n_processes, regions, restricted)
    launch_new_instances(nhpr, instance_type)

    print('waiting for transition from pending to running')
    wait('running', regions)

    print('generating keys')
    # generate signing and keys
    generate_signing_keys(n_processes)

    print('generating addresses file')
    # prepare address file
    ip_list = instances_ip(regions)
    with open('ip_addresses', 'w') as f:
        f.writelines([ip+'\n' for ip in ip_list])

    print('waiting till ports are open on machines')
    # this is really slow, and actually machines are ready earlier! refactor
    #wait('ssh ready', regions)
    sleep(120)

    # TODO try to prevent output of apt spoiling to console
    print('installing dependencies')
    # install dependencies on hosts
    run_task('inst-dep', regions, parallel)

    # TODO check if it works of more than 1 machine per region
    print('wait till installation finishes')
    # wait till installing finishes
    sleep(60)
    wait_install(regions)

    print('packing local repo')
    # pack testing repo
    with Connection('localhost') as c:
        zip_repo(c)

    print('sending testing repo')
    # send testing repo
    run_task('send-testing-repo', regions, parallel)

    print('syncing files')
    # send files: addresses, signing_keys, light_nodes_public_keys
    run_task('sync-files', regions, parallel)

    print('sending parameters')
    # send parameters
    run_task('send-params', regions, parallel)

    print(f'establishing the environment took {round(time()-start, 2)}s')
    # run the experiment
    run_task('run-protocol', regions, parallel)


def get_logs(n_processes, regions, n_parents, use_tcoin, create_freq, sync_init_freq, n_recv_sync, txpu):
    '''Retrieves all logs from instances.'''

    if not os.path.exists('../results'):
        os.makedirs('../results')

    l = len(os.listdir('../results'))
    if l:
        print('sth is in dir ../results; aborting')
        return

    for rn in regions:
        print('collecting logs in ', rn)
        for ip in instances_ip_in_region(rn):
            run_task_for_ip('get-logs', [ip], parallel=0)
            if len(os.listdir('../results')) > l:
                l = len(os.listdir('../results'))
                break

    print(len(os.listdir('../results')), 'files in ../results')

    print('read addresses')
    with open('ip_addresses', 'r') as f:
        ip_addresses = [line[:-1] for line in f]

    print('read signing keys')
    with open('signing_keys', 'r') as f:
        hexes = [line[:-1].encode() for line in f]
        signing_keys = [SigningKey(hexed) for hexed in hexes]

    pk_hexes = [VerifyKey.from_SigningKey(sk).to_hex() for sk in signing_keys]
    arg_sort = [i for i, _ in sorted(enumerate(pk_hexes), key = lambda x: x[1])]

    signing_keys = [signing_keys[i] for i in arg_sort]
    ip_addresses= [ip_addresses[i] for i in arg_sort]

    print('write addresses')
    with open('ip_addresses_sorted', 'w') as f:
        for ip in ip_addresses:
            f.write(ip+'\n')

    print('write signing keys')
    with open('signing_keys_sorted', 'w') as f:
        for sk in signing_keys:
            f.write(sk.to_hex().decode()+'\n')

    print('rename logs')
    for fp in os.listdir('../results'):
        name = fp[-13:-8] # other | aleph
        pid = ip_addresses.index(fp.split(f'-{name}.log')[0].replace('-', '.'))
        os.rename(f'../results/{fp}', f'../results/{pid}.{name}.log.zip')

    result_path = f'../{n_processes}_{n_parents}_{use_tcoin}_{create_freq}_{sync_init_freq}_{n_recv_sync}_{txpu}'

    print('rename dir')
    os.rename('../results', result_path)

    for path in os.listdir(result_path):
        index = path.split('.')[0]
        path = os.path.join(result_path, path)
        with zipfile.ZipFile(path, 'r') as zf:
            zf.extractall(result_path)
        os.rename(f'{result_path}/aleph.log', f'{result_path}/{index}.aleph.log')
        os.remove(path)

    with zipfile.ZipFile(result_path+'.zip', 'w') as zf:
        for path in os.listdir(result_path):
            path = os.path.join(result_path, path)
            zf.write(path)
            os.remove(path)

    os.rmdir(result_path)


def memory_usage(regions=badger_regions()):
    cmd = 'grep memory proof-of-concept/aleph/aleph.log | tail -1'
    output = run_cmd(cmd, regions, True)
    results = [float(line.split()[7]) for line in output]
    return round(min(results), 2), round(np.mean(results), 2), round(max(results), 2)


def reached_max_level(regions=badger_regions()):
    cmd = 'grep max_level proof-of-concept/aleph/aleph.log'
    output = run_cmd(cmd, regions, True, True)
    n_protocol_stopped = 0
    for out in output:
        if len(out.decode().split('reached')) > 1:
            n_protocol_stopped += 1

    return n_protocol_stopped




#======================================================================================
#                                        shortcuts
#======================================================================================

tr = run_task_in_region
t = run_task

cmr = run_cmd_in_region
cm = run_cmd

ti = terminate_instances

restricted = {'ap-south-1':     10,  # Mumbai
              'ap-southeast-2': 5,   # Sydney
              'eu-central-1':   10,  # Frankfurt
              'sa-east-1':      5}   # Sao Paolo
badger_restricted = {'ap-southeast-2': 5, 'sa-east-1': 5}


pb = lambda : run_protocol(104, badger_regions(), [], 't2.medium')
rs = lambda : run_protocol(8, badger_regions(), restricted, 't2.micro')
rf = lambda : run_protocol(128, available_regions(), restricted, 't2.micro')
mu = lambda regions=badger_regions(): memory_usage(regions)

#======================================================================================
#                                         main
#======================================================================================

if __name__=='__main__':
    assert os.getcwd().split('/')[-1] == 'aws', 'Wrong dir! go to experiments/aws'

    from IPython import embed
    from traitlets.config import get_config
    c = get_config()
    c.InteractiveShellEmbed.colors = "Linux"
    embed(config=c)
