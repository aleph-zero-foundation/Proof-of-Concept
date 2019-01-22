import boto3
import os

from fabric import executor
from functools import partial
from joblib import Parallel, delayed
from subprocess import call, check_output
from time import sleep


from utils import image_id_in_region, default_region_name, init_key_pair, security_group_id_by_region, generate_key_pair_all_regions, available_regions, badger_regions, generate_signing_keys
from config import N_JOBS


#======================================================================================
#                              routines for instances
#======================================================================================

def wait_for_instances(insts, target_state):
    wait = True
    while wait:
        sleep(1)
        for i in insts:
            i.load()
        wait = not all(i.state['Name']==target_state for i in insts)


#======================================================================================
#                              routines for some region
#======================================================================================

def launch_new_instances_in_region(n_hosts=1, region_name='default'):
    if region_name == 'default':
        region_name = default_region_name()

    #print(region_name, 'key init')
    key_name = 'aleph'
    init_key_pair(region_name, key_name)

    #print(region_name, 'sg init')
    security_group_name = 'ssh'
    security_group_id = security_group_id_by_region(region_name, security_group_name)

    #print(region_name, 'image init')
    image_id = image_id_in_region(region_name)

    #print(region_name, 'launch instance')
    ec2 = boto3.resource('ec2', region_name)
    ec2.create_instances(ImageId=image_id,
                                 MinCount=n_hosts, MaxCount=n_hosts,
                                 InstanceType='t2.micro',
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
    #print(region_name, 'done')


def terminate_instances_in_region(region_name='default'):
    if region_name == 'default':
        region_name = default_region_name()
    ec2 = boto3.resource('ec2', region_name)
    print(region_name, 'terminating')
    for instance in ec2.instances.all():
        instance.terminate()


def instances_in_region(region_name='default'):
    if region_name == 'default':
        region_name = default_region_name()
    ec2 = boto3.resource('ec2', region_name)
    instances = []
    print(region_name, 'collecting instances')
    for instance in ec2.instances.all():
        if instance.state['Name'] in ['running', 'pending']:
            instances.append(instance)

    return instances


def instances_ip_in_region(region_name='default'):
    if region_name == 'default':
        region_name = default_region_name()
    ec2 = boto3.resource('ec2', region_name)
    ips = []
    print(region_name, 'collecting ips')
    for instance in ec2.instances.all():
        if instance.state['Name'] in ['running', 'pending']:
            ips.append(instance.public_ip_address)

    return ips


def instances_state_in_region(region_name='default'):
    if region_name == 'default':
        region_name = default_region_name()
    ec2 = boto3.resource('ec2', region_name)
    states = []
    print(region_name, 'collecting instances states')
    for instance in ec2.instances.all():
        states.append(instance.state['Name'])

    return states


def run_cmd_in_region(cmd='ls', region_name='default', output=False):
    if region_name == 'default':
        region_name = default_region_name()
    ip_list = instances_ip_in_region(region_name)
    results = []
    for ip in ip_list:
        cmd_ = f'ssh -o "StrictHostKeyChecking no" -i key_pairs/aleph.pem ubuntu@{ip} -t "{cmd}"'
        if output:
            results.append(check_output(cmd_, shell=True))
        else:
            results.append(call(cmd_, shell=True))

    return results


def run_task_in_region(task='test', region_name='default', parallel=False, output=False):
    if region_name == 'default':
        region_name = default_region_name()
    ip_list = instances_ip_in_region(region_name)
    if parallel:
        hosts = " ".join(["ubuntu@"+ip for ip in ip_list])
        cmd = 'parallel fab -i key_pairs/aleph.pem -H {} '+task+' ::: '+hosts
    else:
        hosts = ",".join(["ubuntu@"+ip for ip in ip_list])
        cmd = f'fab -i key_pairs/aleph.pem -H {hosts} {task}'
    print(region_name, f'calling {cmd}')
    if output:
        return check_output(cmd.split())
    return call(cmd.split())


def wait_in_region(target_state, region_name):
    if region_name == 'default':
        region_name = default_region_name()
    instances = boto3.resource('ec2', region_name).instances.all()
    wait = True
    print(region_name, 'waiting')
    while wait:
        wait = not all(inst.state['Name'] in target_state for inst in instances)


def installation_finished_in_region(region_name):
    ip_list = instances_ip_in_region(region_name)
    results = []
    cmd = "tail -1 setup.log"
    results = run_cmd_in_region(cmd, region_name, output=True)
    for result in results:
        if len(result) < 4 or result[:4] != b'done':
            print(region_name, result)
            return False

    return True

#======================================================================================
#                              routines for all regions
#======================================================================================

def exec_for_regions(func, regions='badger regions', parallel=True):
    if regions == 'badger regions':
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


def launch_new_instances(n_hosts_per_region=1, regions='badger regions'):
    return exec_for_regions(partial(launch_new_instances_in_region, n_hosts_per_region), regions)


def terminate_instances(regions='badger regions'):
    return exec_for_regions(terminate_instances_in_region, regions)


def instances(regions='badger regions'):
    return exec_for_regions(instances_in_region, regions, parallel=False)


def instances_ip(regions='badger regions'):
    return exec_for_regions(instances_ip_in_region, regions)


def instances_state(regions='badger regions'):
    return exec_for_regions(instances_state_in_region, regions)


def run_task(task='test', regions='badger regions', parallel=False, output=False):
    return exec_for_regions(partial(run_task_in_region, task, parallel=parallel, output=output), regions)


def run_cmd(cmd='ls', regions='badger regions', output=False):
    return exec_for_regions(partial(run_cmd_in_region, cmd), regions)

def wait(target_state, regions='badger regions'):
    exec_for_regions(partial(wait_in_region, target_state), regions)

def wait_install(regions='badger regions'):
    wait_for_regions = regions.copy()
    while wait_for_regions:
        sleep(1)
        finished = []
        results = Parallel(n_jobs=N_JOBS)(delayed(installation_finished_in_region)(r) for r in wait_for_regions)

        wait_for_regions = [r for i,r in enumerate(wait_for_regions) if not results[i]]


#======================================================================================
#                               aggregates
#======================================================================================

def run_experiment(n_processes, regions, experiment):
    parallel = n_processes > 1
    if regions == 'badger_regions':
        regions = badger_regions()
    if regions == 'all':
        regions = available_regions()

    # TODO there is some wierd exception thrown by launch_new_instances
    n_proc_per_region = n_processes//len(regions)
    launch_new_instances(n_proc_per_region, regions)
    if n_processes % len(regions):
        launch_new_instances_in_region(n_processes%len(regions), default_region_name())

    print('waiting for transition from pending to running')
    insts = instances(regions)
    wait_for_instances(insts, 'running')

    print('generating keys')
    # generate signing and keys
    generate_signing_keys(n_processes)

    print('generating hist')
    # prepare hosts file
    ip_list = instances_ip(regions)
    with open('hosts', 'w') as f:
        f.writelines([ip+'\n' for ip in ip_list])

    print('waiting till ports are open on machines')
    sleep(15)

    print('syncing files')
    # send files: hosts, signing_keys, setup.sh, set_env.sh
    run_task('sync-files', regions, parallel)

    print('installing dependencies')
    # install dependencies on hosts
    run_task('inst-dep', regions, parallel)

    print('wait till installation finishes')
    # wait till installing finishes
    wait_install(regions)

    print('packing local repo')
    # pack testing repo
    call('fab -H 127.0.0.1 zip-repo'.split())

    print('sending testing repo')
    # send testing repo
    run_task('send-testing-repo', regions, parallel)

    # run the experiment
    # TODO



#======================================================================================
#                               aggregates
#======================================================================================





#======================================================================================
#                                         main
#======================================================================================

if __name__=='__main__':
    assert os.getcwd().split('/')[-1] == 'aws', 'Wrong dir! go to experiments/aws'

    import IPython; IPython.embed()
#     try:
#         __IPYTHON__
#     except NameError:
#         import IPython
#         IPython.embed()
