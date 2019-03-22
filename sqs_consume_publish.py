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
from unpack_archive import NotSupportedFileType, unpack_archive

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
    client = boto3.client('sqs')
    s3 = boto3.resource('s3')

    while True:
        try:
            logger.info("sqs.receive_message " + SqsUrl)
            response = client.receive_message(QueueUrl=SqsUrl)
            response_code = response['ResponseMetadata']['HTTPStatusCode']
            logger.debug('response_code = %s' % response_code)
            try:
                message_body = response['Messages'][0]['Body']
            except KeyError:
                logger.info("KeyError MessageBody not exist  %s" % pformat(response))
                continue
            receipt_handle = response['Messages'][0]['ReceiptHandle']
            logger.debug("Deleting Message: %s" % receipt_handle)
            try:
                client.delete_message(
                    QueueUrl=SqsUrl,
                    ReceiptHandle=receipt_handle,
                )
            except ClientError as e:
                logger.warning("delete_message error: %s" % pformat(e))

            bucket = json.loads(message_body)['bucket']
            key = json.loads(message_body)['key']
            logger.debug('key=' + key)

            local_file = pjoin(dl_dir, key.split('/')[-1])
            try:
                keysize = s3.Bucket(bucket).Object(key).content_length
                logger.info('download size=%s s3://%s/%s' % (humanfriendly.format_size(keysize), bucket, key))
                s3.Bucket(bucket).download_file(key, local_file)
            except ClientError as e:
                if e.response['Error']['Code'] == "404":
                    logger.warning('Bucket Key Not exist: ' + key)
                else:
                    logger.warning('Key "%s" exception %s' % (key, pformat(e)))
            except Exception as e:
                logger.warning(pformat(e))
                traceback.print_exc()
                continue

            try:
                os.makedirs(ext_dir, exist_ok=True)
                children = [pjoin(ext_dir, f) for f, _ in unpack_archive(abspath(local_file), abspath(ext_dir))]
                logger.info('prepared to upload %s files', len(children))
                proc = subprocess.Popen("du '%s' -sh" % ext_dir, shell=True, stdout=subprocess.PIPE,
                                        universal_newlines=True, bufsize=1)
                du_str, _ = proc.communicate(timeout=1.0)
                logger.info('extracted file size is %s', du_str.split()[0])
                for f in children:
                    child_key = '_extracted/' + guen_keyname(getSha256(f))
                    try:
                        logger.debug('upload child "%s" to s3://%s/%s"' % (f, bucket, child_key))
                        s3.Bucket(bucket).upload_file(f, child_key)
                    except ClientError as e:
                        logger.warning('Failed to upload(%s, %s) %s' % (f, child_key, pformat(e)))
            except NotSupportedFileType:
                logger.warning('NotSupportedFileType: %s' % local_file)
                shutil.rmtree(ext_dir, ignore_errors=True)
            except Exception as e:
                logger.warning(pformat(e))
                traceback.print_exc()
                shutil.rmtree(ext_dir, ignore_errors=True)
            else:
                shutil.rmtree(ext_dir, ignore_errors=True)
                os.remove(local_file)
        except KeyboardInterrupt:
            logger.info('broken by user')
            return
        except Exception as e:
            logger.warning(pformat(e))
            logger.warning(traceback.format_exc())


if __name__ == '__main__':
    main()
