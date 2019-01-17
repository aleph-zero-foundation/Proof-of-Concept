import boto3
from subprocess import call

from utils import image_id_in_region, default_region_name, init_key_pair, security_group_id_by_region, generate_key_pair_all_regions, available_regions


def launch_new_instances(count=1, region_name='default'):
    from time import time
    start = time()
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
    ec2 = boto3.client('ec2', region_name)
    instance = ec2.run_instances( ImageId=image_id, MinCount=count, MaxCount=count, InstanceType='t2.micro', BlockDeviceMappings=[ { 'DeviceName': '/dev/xvda', 'Ebs': { 'DeleteOnTermination': True, 'VolumeSize': 8, 'VolumeType': 'gp2' }, }, ], KeyName=key_name, Monitoring={ 'Enabled': False }, SecurityGroupIds = [security_group_id])
    print('launching took', round(time()-start,2))
    return instance


def terminate_instances(region_name):
    ec2 = boto3.resource('ec2', region_name)
    for instance in ec2.instances.all():
        instance.terminate()


def instances_id(region_name='default'):
    if region_name == 'default':
        region_name = default_region_name()
    ec2 = boto3.resource('ec2', region_name)
    ids = []
    for instance in ec2.instances.all():
        ids.append(instance.id)

    return ids


def instances_ip(region_name='default'):
    if region_name == 'default':
        region_name = default_region_name()
    ec2 = boto3.resource('ec2', region_name)
    ips = []
    for instance in ec2.instances.all():
        ips.append(instance.public_ip_address)

    return ips


def run_fab(ip_list=None, task='test'):
    if ip_list is None:
        ip_list = []
        for region_name in available_regions():
            ip_list.extend(instances_ip(region_name))

    call(f'fab -i key_pairs/aleph.pem -H ubuntu@{",".join(ip_list)} {task}'.split())

if __name__=='__main__':
    import IPython; IPython.embed()
#     try:
#         __IPYTHON__
#     except NameError:
#         import IPython
#         IPython.embed()
