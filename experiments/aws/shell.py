import boto3
import os

from functools import partial
from subprocess import call


from utils import image_id_in_region, default_region_name, init_key_pair, security_group_id_by_region, generate_key_pair_all_regions, available_regions, badger_regions, generate_signing_keys




#======================================================================================
#                              routines for some region
#======================================================================================

def launch_new_instances_in_region(n_hosts=1, region_name='default'):
    if region_name == 'default':
        region_name = default_region_name()

    print('key init')
    key_name = 'aleph'
    init_key_pair(region_name, key_name)

    print('sg init')
    security_group_name = 'ssh'
    security_group_id = security_group_id_by_region(region_name, security_group_name)

    print('image init')
    image_id = image_id_in_region(region_name)

    print('run instance')
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
    return instance


def terminate_instances_in_region(region_name='default'):
    if region_name == 'default':
        region_name = default_region_name()
    ec2 = boto3.resource('ec2', region_name)
    for instance in ec2.instances.all():
        instance.terminate()


def instances_id_in_region(region_name='default'):
    if region_name == 'default':
        region_name = default_region_name()
    ec2 = boto3.resource('ec2', region_name)
    ids = []
    for instance in ec2.instances.all():
        ids.append(instance.id)

    return ids


def instances_ip_in_region(region_name='default'):
    if region_name == 'default':
        region_name = default_region_name()
    ec2 = boto3.resource('ec2', region_name)
    ips = []
    for instance in ec2.instances.all():
        ips.append(instance.public_ip_address)

    return ips


def instances_state_in_region(region_name='defalut'):
    if region_name == 'default':
        region_name = default_region_name()
    ec2 = boto3.resource('ec2', region_name)
    states = []
    for instance in ec2.instances.all():
        states.append(instance.state)

    return states


def run_task_in_region(task='test', region_name='default'):
    ip_list = instances_ip_in_region(region_name)
    hosts = ",".join(["ubuntu@"+ip for ip in ip_list])
    call(f'fab -i key_pairs/aleph.pem -H {hosts} {task}'.split())


def init_region(n_processes=4, region_name='default'):
    # gen signing and keys
    generate_signing_keys(n_processes)
    # prepare hosts file
    ip_list = instances_ip_in_region(region_name)
    with open('hosts', 'w') as f:
        f.writelines([ip+'\n' for ip in ip_list])

    # launch machines
    launch_new_instances_in_region(n_processes, region_name)

    # prepare machines
    run_task_in_region('init', region_name)


#======================================================================================
#                              routines for all regions
#======================================================================================

def exec_for_regions(func, regions='badger regions'):
    if regions == 'badger regions':
        regions = available_regions()
    if regions == 'badger regions':
        regions = badger_regions()

    results = []
    for region_name in regions:
        print(f'executing {func} in region {region_name}')
        result = func(region_name)
        if isinstance(result, list):
            results.extend(result)
        else:
            results.append(result)

    return results


def launch_new_instances(n_hosts=1, regions='badger regions'):
    return exec_for_regions(partial(launch_new_instances, n_hosts), regions)


def terminate_instances(regions='badger regions'):
    return exec_for_regions(terminate_instances_in_region, regions)


def instances_id(regions='badger regions'):
    return exec_for_regions(instances_id_in_region, regions)


def instances_ip(regions='badger regions'):
    return exec_for_regions(instances_ip_in_region, regions)


def instances_state(regions='badger regions'):
    return exec_for_regions(instances_state_in_region, regions)


def run_task(task='test', regions='badger regions'):
    exec_for_regions(partial(task), regions)


def init(n_processes=4, regions='badger regions'):
    exec_for_regions(partial(init_region, n_processes), regions)



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
