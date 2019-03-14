#!/usr/bin/env python3
# coding: utf-8
import os
# import re
import json
import hashlib
from collections import OrderedDict
import boto3



def upload_file(f_url, local_f, contentType, lastModified):
    production_phase = True
    if production_phase == True:
        bucketName = 'grid-linux-harvest'
        aws_service_type = 'sqs'
        sqs_url='https://sqs.us-west-2.amazonaws.com/745063655428/grid_linux_harvest'
    else:
        bucketName = 'grid-staging-linux'
        aws_service_type = 'sns'
        topicArn = 'arn:aws:sns:us-east-1:934030439160:grid_ux_harvest_staging'

    md5 = hashlib.md5()
    sha1 = hashlib.sha1()
    sha256 = hashlib.sha256()
    with open(local_f, 'rb') as f:
        while True:
            data = f.read(1024*1024)
            if not data:
                break
            md5.update(data)
            sha1.update(data)
            sha256.update(data)

    key = sha256.hexdigest()
    key = key[:2] + '/' + key[2:5] + '/' + key[5:8] + '/' + key
    contentTag = contentType
    bucket = boto3.resource('s3', region_name='us-west-2').Bucket(bucketName)
    bucket.upload_file(local_f, key)

    msg = OrderedDict([
        ('bucket', bucketName), ('key', key), ('sha1', sha1.hexdigest()),
        ('md5', md5.hexdigest()), ('sha256', sha256.hexdigest()), ('priority', 5),
        ('source', f_url), ('contentTag', contentTag), ('lastModified', lastModified),
        ('filename', os.path.basename(local_f)),
        ("sourceCategory", "InternalPartner/GRID-UX"),
        ('jobRegistryId', 'na')])

    response = None
    if aws_service_type == 'sns':
        client = boto3.client('sns', region_name='us-east-1')
        response = client.publish(TopicArn='arn:aws:sns:us-east-1:934030439160:grid_ux_harvest_staging', Message=json.dumps(msg))
    else:
        client = boto3.client('sqs', region_name='us-west-2')
        response = client.send_message(QueueUrl='https://sqs.us-west-2.amazonaws.com/745063655428/grid_linux_harvest', MessageBody=json.dumps(msg))
    response_code = response['ResponseMetadata']['HTTPStatusCode']
    if response_code == 200:
        return True
    else:
        print('Upload failed, reponse = %s' % response)
        return False
