#!/usr/bin/env python3
# -*- coding: utf8 -*-
import os
from os.path import join as pjoin
from os.path import abspath
import sys
import json
import logging
import traceback
import shutil
import subprocess
from pprint import pformat
import boto3
from botocore.exceptions import ClientError
import humanfriendly
from tqdm import tqdm
from unpack_archive import NotSupportedFileType, unpack_archive, chown_to_me

logger = logging.getLogger(__name__)

SqsUrl = 'https://sqs.us-east-1.amazonaws.com/934030439160/cloud_staging_ux_sns04_guenlinux'
dl_dir = 'downloads'
ext_dir = 'extracted'


def getSha256(fname: str) -> str:
    import hashlib
    sha256 = hashlib.sha256()
    with open(fname, 'rb') as f:
        while True:
            data = f.read(1024 * 1024)
            if not data:
                break
            sha256.update(data)
    return sha256.hexdigest()


def guen_keyname(key: str) -> str:
    return key[:2] + '/' + key[2:5] + '/' + key[5:8] + '/' + key


def main():
    logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s %(message)s",
                        stream=sys.stdout, level=logging.INFO)

    os.makedirs(dl_dir, exist_ok=True)
    sqs = boto3.client('sqs')
    s3 = boto3.resource('s3')

    try:
        with open('.sqs_step', 'r') as fin:
            step = fin.readline().strip()
            if step == 'recvMsg':
                pass
            elif step == 'delMsg':
                remains = fin.read()
                response = json.loads(remains)
            elif step == 'downloadFile':
                remains = fin.readline()
                bucket, key = [_.strip() for _ in remains.split(' ', maxsplit=1)]
            elif step == 'unpack_archive':
                local_file = fin.readline().strip()
                bucket = fin.readline().strip()
            elif step == 'getDirSize':
                ext_total_bytes = int(fin.readline().strip())
                bucket = fin.readline().strip()
            elif step == 'uploadFiles':
                ext_total_bytes = int(fin.readline().strip())
                bucket = fin.readline().strip()
                children = list()
                for l in fin:
                    l = l.strip()
                    if not l:
                        continue
                    children.append(l)
    except FileNotFoundError:
        step = ''

    def hook(t: tqdm):
        def inner(bytes_amount):
            t.update(bytes_amount)
        return inner

    while True:
        try:
            if step in ['', 'end', 'recvMsg']:
                step = 'recvMsg'
                logger.info("sqs.receive_message " + SqsUrl)
                response = sqs.receive_message(QueueUrl=SqsUrl)
                response_code = response['ResponseMetadata']['HTTPStatusCode']
                logger.debug('response_code = %s' % response_code)
                try:
                    message_body = response['Messages'][0]['Body']
                except (KeyError, IndexError):
                    logger.info("MessageBody not exist  %s" % pformat(response))
                    continue

            if step in ['recvMsg', 'delMsg']:
                if step == 'delMsg':
                    assert response
                step = 'delMsg'
                receipt_handle = response['Messages'][0]['ReceiptHandle']
                logger.debug("Delete Message: %s" % receipt_handle)
                try:
                    sqs.delete_message(QueueUrl=SqsUrl, ReceiptHandle=receipt_handle)
                except ClientError as e:
                    logger.warning("delete_message error: %s" % pformat(e))

            if step in ['delMsg', 'downloadFile']:
                if step == 'downloadFile':
                    assert bucket
                    assert key
                elif step == 'delMsg':
                    bucket = json.loads(message_body)['bucket']
                    key = json.loads(message_body)['key']
                step = 'downloadFile'
                logger.debug('key=' + key)
                local_file = pjoin(dl_dir, key.split('/')[-1])
                try:
                    s3_obj = s3.Bucket(bucket).Object(key)
                    fileSize = s3_obj.content_length
                    logger.info('download size=%s s3://%s/%s' % (humanfriendly.format_size(fileSize), bucket, key))
                    with tqdm(total=fileSize, ncols=100, leave=False) as t:
                        s3_obj.download_file(local_file, Callback=hook(t))
                except ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        logger.warning('Bucket Key Not exist: ' + key)
                    else:
                        logger.warning('Key "%s" exception %s' % (key, pformat(e.response)))

            if step in ['downloadFile', 'unpack_archive']:
                if step == 'unpack_archive':
                    assert local_file
                step = 'unpack_archive'
                try:
                    os.makedirs(ext_dir, exist_ok=True)
                    children = [pjoin(ext_dir, f) for f, _ in unpack_archive(abspath(local_file), abspath(ext_dir))]
                except NotSupportedFileType:
                    if 'ASCII text' in e.ftype or 'XML document text' in e.ftype \
                            or 'PGP signature' in e.ftype or 'JPEG image' in e.ftype \
                            or 'PNG image' in e.ftype:
                        children = [local_file]
                    else:
                        logger.warning('NotSupportedFileType: %s' % local_file)
                        chown_to_me(ext_dir)
                        shutil.rmtree(ext_dir)
                        step = 'end'
                        continue
                os.remove(local_file)

            if step in ['getDirSize', 'unpack_archive']:
                step = 'getDirSize'
                proc = subprocess.Popen("du %s -sb" % ext_dir, shell=True, stdout=subprocess.PIPE,
                                        universal_newlines=True, bufsize=1)
                du_str, _ = proc.communicate()
                ext_total_bytes = int(du_str.split()[0])
                logger.info('extracted file size is %s', humanfriendly.format_size(ext_total_bytes))

            if step in ['uploadFiles', 'getDirSize']:
                if step == 'uploadFiles':
                    assert children
                step = 'uploadFiles'
                try:
                    logger.info('prepared to upload %s files' % len(children))
                    with tqdm(total=ext_total_bytes, unit='B', unit_scale=True, desc='upload', ncols=100, leave=False) as t:
                        for f in children:
                            try:
                                child_key = '_extracted/' + guen_keyname(getSha256(f))
                            except FileNotFoundError:
                                logger.info('File not found: ' + f)
                                continue
                            try:
                                logger.debug('upload child "%s" to s3://%s/%s"' % (f, bucket, child_key))
                                fileSize = os.path.getsize(f)
                                s3.Bucket(bucket).upload_file(f, child_key, Callback=hook(t))
                            except ClientError as e:
                                logger.warning('Failed to upload(%s, %s) %s' % (f, child_key, pformat(e)))
                except Exception as e:
                    logger.warning(pformat(e))
                    traceback.print_exc()
                chown_to_me(ext_dir)
                shutil.rmtree(ext_dir)

            step = 'end'
        except (KeyboardInterrupt, Exception) as e:
            if isinstance(e, KeyboardInterrupt):
                logger.info('broken by user')
            else:
                logger.warning(pformat(e))
                logger.warning(traceback.format_exc())
            with open('.sqs_step', 'w') as fout:
                fout.write(step + '\n')
                if step == 'recvMsg':
                    pass
                elif step == 'delMsg':
                    fout.write(json.dumps(response, indent=2))
                elif step == 'downloadFile':
                    fout.write("%s %s\n" % (bucket, key))
                elif step == 'unpack_archive':
                    fout.write(local_file + '\n')
                    fout.write(bucket + '\n')
                elif step == 'getDirSize':
                    fout.write('%s\n' % ext_total_bytes)
                    fout.write(bucket + '\n')
                elif step == 'uploadFiles':
                    fout.write('%s\n' % ext_total_bytes)
                    fout.write(bucket + '\n')
                    for f in children:
                        fout.write(f + '\n')
            return


if __name__ == '__main__':
    main()
