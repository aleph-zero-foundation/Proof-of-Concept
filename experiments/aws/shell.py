import boto3
from subprocess import call

from utils import image_id_in_region, default_region_name, init_key_pair, security_group_id_by_region, generate_key_pair_all_regions, available_regions


def launch_new_instances_in_region(count=1, region_name='default'):
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
                                 MinCount=count, MaxCount=count,
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


def terminate_instances_in_region(region_name):
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


def exec_for_regions(func, regions='all'):
    if regions == 'all':
        regions = available_regions()

    results = []
    for region_name in regions:
        print(f'executing {func} in region {region_name}')
        result = func(region_name)
        if isinstance(result, list):
            results.extend(result)
        else:
            results.append(result)

    return results


def launch_new_instances(count=1, regions='all'):
    def func(region_name):
        return launch_new_instances_in_region(count, region_name)

    return exec_for_regions(func, regions)


def terminate_instances(regions='all'):
    return exec_for_regions(terminate_instances_in_region, regions)


def instances_id(regions='all'):
    return exec_for_regions(instances_id_in_region, regions)


def instances_ip(regions='all'):
    return exec_for_regions(instances_ip_in_region, regions)


def instances_state(regions='all'):
    return exec_for_regions(instances_state_in_region, regions)


def run_fab(ip_list=None, task='test'):
    if ip_list is None:
        ip_list = instances_ip()

    call(f'fab -i key_pairs/aleph.pem -H {",".join(["ubuntu@"+ip for ip in ip_list])} {task}'.split())


if __name__=='__main__':
    import IPython; IPython.embed()
#     try:
#         __IPYTHON__
#     except NameError:
#         import IPython
#         IPython.embed()
