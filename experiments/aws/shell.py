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

#======================================================================================
#                              routines for some region
#======================================================================================

def launch_new_instances_in_region(n_hosts=1, region_name='default'):
    if region_name == 'default':
        region_name = default_region_name()

    print(region_name, 'key init')
    key_name = 'aleph'
    init_key_pair(region_name, key_name)

    print(region_name, 'sg init')
    security_group_name = 'ssh'
    security_group_id = security_group_id_by_region(region_name, security_group_name)

    print(region_name, 'image init')
    image_id = image_id_in_region(region_name)

    print(region_name, 'launch instance')
    ec2 = boto3.resource('ec2', region_name)
    instance = ec2.create_instances(ImageId=image_id,
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
    print(region_name, 'done')
    return instance


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
            instances.append(instance.id)

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


def run_task_in_region(task='test', region_name='default', parallel=False, output=False):
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
    return all(check_output(
        f'ssh -i key_pairs/aleph.pem ubuntu@{ip} -t "tail -1 setup.log"', shell=True
    )[:4]==b'done' for ip in ip_list)

#======================================================================================
#                              routines for all regions
#======================================================================================

def exec_for_regions(func, regions='badger regions'):
    if regions == 'badger regions':
        regions = available_regions()
    if regions == 'badger regions':
        regions = badger_regions()

    results = Parallel(n_jobs=N_JOBS)(delayed(func)(region_name) for region_name in regions)

    if results and isinstance(results[0], list):
        return [res for res_list in results for res in res_list]

    return results


def launch_new_instances(n_hosts_per_region=1, regions='badger regions'):
    return exec_for_regions(partial(launch_new_instances_in_region, n_hosts_per_region), regions)


def terminate_instances(regions='badger regions'):
    return exec_for_regions(terminate_instances_in_region, regions)


def instances(regions='badger regions'):
    return exec_for_regions(instances_in_region, regions)


def instances_ip(regions='badger regions'):
    return exec_for_regions(instances_ip_in_region, regions)


def instances_state(regions='badger regions'):
    return exec_for_regions(instances_state_in_region, regions)


def run_task(task='test', regions='badger regions', parallel=False, output=False):
    return exec_for_regions(partial(run_task_in_region, task, parallel=parallel, output=output), regions)


def wait(target_state, regions='badger regions'):
    exec_for_regions(partial(wait_in_region, target_state), regions)


#======================================================================================
#                               aggregates
#======================================================================================

def run_experiment(n_processes, regions, experiment):
    parallel = n_processes > 1
    if regions == 'badger_regions':
        regions = badger_regions()
    if regions == 'all':
        regions = available_regions()

    n_proc_per_region = n_processes//len(regions)
    launch_new_instances(n_proc_per_region, regions)
    if n_processes % len(regions):
        launch_new_instances(n_processes%len(regions), default_region_name())

    wait('running', regions)

    # generate signing and keys
    generate_signing_keys(n_processes)

    # prepare hosts file
    ip_list = instances_ip(regions)
    with open('hosts', 'w') as f:
        f.writelines([ip+'\n' for ip in ip_list])

    # send files: hosts, signing_keys, setup.sh, set_env.sh
    run_task('sync-files', regions, parallel)

    # install dependencies on hosts
    run_task('inst-dep', regions, parallel)

    # wait till installing finishes
    wait_for_regions = regions.copy()
    while wait_for_regions:
        sleep(1)
        finished = []
        results = Parallel(n_jobs=N_JOBS)(delayed(installation_finished_in_region)(r) for r in wait_for_regions)

        for i, finished in enumerate(results):
            if finished:
                del wait_for_regions[i]

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
