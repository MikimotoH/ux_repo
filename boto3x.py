#!/usr/bin/env python3
# coding: utf-8
import os
# import re
import json
import hashlib
from collections import OrderedDict
import boto3


bucketName = 'grid-harvest-uploads'


def upload_file(f_url, local_f, contentType, lastModified):
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
    bucket = boto3.resource('s3').Bucket(bucketName)
    bucket.upload_file(local_f, key)

    msg = OrderedDict([
        ('bucket', bucketName), ('key', key), ('sha1', sha1.hexdigest()),
        ('md5', md5.hexdigest()), ('sha256', sha256.hexdigest()), ('priority', 5),
        ('source', f_url), ('contentTag', contentTag), ('lastModified', lastModified),
        ('filename', os.path.basename(local_f)),
        ("sourceCategory", "InternalPartner/GRID-UX"),
        ('jobRegistryId', 'na')])

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(
        QueueName='harvest_gridux',
        QueueOwnerAWSAccountId="934030439160")
    queue.send_message(MessageBody=json.dumps(msg))
