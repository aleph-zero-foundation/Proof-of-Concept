import boto3
import os
from pathlib import Path
from subprocess import check_output, call


def _def_filter(name, values):
    return {'Name': name, 'Values': values}


def image_id_in_region(region_name):
    '''Find id of os image we use. It may differ for different regions'''
    image_name = 'ubuntu/images/hvm-ssd/ubuntu-bionic-18.04-amd64-server-20181203'
    ec2 = boto3.resource('ec2', region_name)
    # below there is only one image in the iterator
    for image in ec2.images.filter(Filters=[{'Name': 'name', 'Values':[image_name]}]):
        return image.id


def vpc_id_in_region(region_name):
    ec2 = boto3.resource('ec2', region_name)
    vpcs_ids = []
    for vpc in ec2.vpcs.all():
        vpcs_ids.append(vpc.id)

    if len(vpcs_ids) > 1 or len(vpcs_ids) == 0:
        raise Exception

    return vpcs_ids[0]


def create_security_group(region_name, security_group_name):
    '''Creates security group that allows connecting via ssh'''
    ec2 = boto3.resource('ec2', region_name)

    vpc_id = vpc_id_in_region(region_name)
    sg = ec2.create_security_group(GroupName=security_group_name, Description='ssh', VpcId=vpc_id)
    sg.authorize_ingress(
        GroupName=security_group_name,
        IpPermissions = [
            {
                'FromPort': 22,
                'IpProtocol': 'tcp',
                'IpRanges': [{'CidrIp': '0.0.0.0/0'}],
                'ToPort': 22,
            }
        ]
    )
    return sg.id


def security_group_id_by_region(region_name, security_group_name):
    '''Finds id of a security group. It may differ for different regions'''
    ec2 = boto3.resource('ec2', region_name)
    security_groups = ec2.security_groups.all()
    for security_group in security_groups:
        if security_group.group_name == security_group_name:
            return security_group.id

    # it seems that the group does not exist, let fix that
    return create_security_group(region_name, security_group_name)


def check_key_uploaded_all_regions(key_name='aleph'):
    '''Checks if in all regions there is public key corresponding to local private key.'''
    key_path = f'key_pairs/{key_name}.pem'
    assert os.path.exists(key_path), 'there is no key locally!'
    fingerprint_path = f'key_pairs/{key_name}.fingerprint'
    assert os.path.exists(fingerprint_path), 'there is no fingerprint of the key!'

    with open(fingerprint_path, 'r') as f:
        fp = f.readline()

    for region_name in available_regions():
        ec2 = boto3.resource('ec2', region_name)
        if not any(key.key_fingerprint==fp for key in ec2.key_pairs.all()):
            return False

    return True


def generate_key_pair_all_regions(key_name='aleph'):
    '''Generates key pair, stores private key locally and sends public key to all regions'''
    key_path = f'key_pairs/{key_name}.pem'
    fingerprint_path = f'key_pairs/{key_name}.fingerprint'
    assert not os.path.exists(key_path), 'key exists, just use it!'

    pk_material, wrote_fp = None, False
    for region_name in available_regions():
        ec2 = boto3.resource('ec2', region_name)
        # first delete the old key
        # TODO refactor using ec2.KeyPair(KeyName='aleph')
        for key in ec2.key_pairs.all():
            if key.name == key_name:
                print(f'deleting old key {key.name} in region', region_name)
                key.delete()
                break
        if pk_material is None:
            print('generating key pair')
            call(f'openssl genrsa -out {key_path} 2048'.split())
            call(f'openssl rsa -in {key_path} -outform PEM -pubout -out {key_path}.pub'.split())
            with open(key_path+'.pub', 'r') as f:
                pk_material = ''.join([line[:-1] for line in f.readlines()[1:-1]])
        print('sending key pair to region', region_name)
        ec2.import_key_pair(KeyName=key_name, PublicKeyMaterial=pk_material)
        if not wrote_fp:
            with open(fingerprint_path, 'w') as f:
                f.write(ec2.KeyPair(key_name).key_fingerprint)
            wrote_fp = True


def init_key_pair(region_name, key_name='aleph'):
    key_path = f'key_pairs/{key_name}.pem'
    fingerprint_path = f'key_pairs/{key_name}.fingerprint'
    if os.path.exists(key_path) and os.path.exists(fingerprint_path):
        print('    found local key; ', end='')
        # we have the private key locally so let check if we have pk in the region
        ec2 = boto3.resource('ec2', region_name)
        with open(fingerprint_path, 'r') as f:
            fp = f.readline()

        keys = ec2.key_pairs.all()
        for key in keys:
            if key.name == key_name:
                if key.key_fingerprint != fp:
                    print('there is old version of key in region', region_name)
                    # there is an old version of the key, let remove it
                    key.delete
                else:
                    print('local and upstream key match')
                    # everything is alright
                    return

        # for some reason there is no key up there, let send it
        with open(key_path+'.pub', 'r') as f:
            lines = f.readlines()
            pk_material = ''.join([line[:-1] for line in f.readlines()[1:-1]])
        ec2.import_key_pair(KeyName=key_name, PublicKeyMaterial=pk_material)
    else:
        # create key
        generate_key_pair_all_regions(key_name)


def read_keys():
    creds_path = str(Path.joinpath(Path.home(), Path('.aws/credentials')))
    with open(creds_path, 'r') as f:
        f.readline() # skip block description
        access_key_id = f.readline().strip().split('=')[-1].strip()
        secret_access_key = f.readline().strip().split('=')[-1].strip()
        return access_key_id, secret_access_key


def available_regions():
    return boto3.Session(region_name='eu-west-1').get_available_regions('ec2')


def default_region_name():
    return boto3.Session().region_name


def running_instances(region_name):
    pass


def describe_instances(region_name):
    ec2 = boto3.resource('ec2', region_name)
    for instance in ec2.instances.all():
        print(f'ami_launch_index={instance.ami_launch_index} state={instance.state}')
