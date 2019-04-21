#!/usr/bin/env python3
# -*- coding: utf8 -*-
import os
import sys
from os.path import abspath, basename, join as pjoin
import shutil
import subprocess
import logging
import traceback
from pprint import pformat
import humanfriendly
from tqdm import tqdm
import boto3
from botocore.exceptions import ClientError
from unpack_archive import unpack_archive, chown_to_me, NotSupportedFileType
from sqs_consume_publish import getSha256, guen_keyname


dl_dir = 'downloads'
ext_dir = 'extracted2'
bucket = 'grid-staging-linux'

logger = logging.getLogger(__name__)


def main():
    s3 = boto3.resource('s3')
    logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s %(message)s",
                        stream=sys.stdout, level=logging.INFO)

    def hook(t: tqdm):
        def inner(bytes_amount):
            t.update(bytes_amount)
        return inner

    for f in os.listdir(dl_dir):
        try:
            local_file = pjoin(dl_dir, f)
            try:
                os.makedirs(ext_dir, exist_ok=True)
                children = [pjoin(ext_dir, f) for f, _ in unpack_archive(local_file, ext_dir)]
            except NotSupportedFileType as e:
                if 'ASCII text' in e.ftype or 'XML document text' in e.ftype \
                        or 'PGP signature' in e.ftype or 'JPEG image' in e.ftype \
                        or 'PNG image' in e.ftype:
                    children = [local_file]
                else:
                    logger.warning('NotSupportedFileType: %s %s' % (e.ftype, f))
                    chown_to_me(ext_dir)
                    shutil.rmtree(ext_dir)
                    continue

            proc = subprocess.Popen("du %s -sb" % ext_dir, shell=True, stdout=subprocess.PIPE,
                                    universal_newlines=True, bufsize=1)
            du_str, _ = proc.communicate()
            ext_total_bytes = int(du_str.split()[0])
            logger.info('extracted file size is %s', humanfriendly.format_size(ext_total_bytes))
            try:
                logger.info('prepared to upload %s files' % len(children))
                with tqdm(total=ext_total_bytes, unit='B', unit_scale=True, desc='upload', ncols=100, leave=False) as t:
                    for f in children:
                        child_key = '_extracted/' + guen_keyname(getSha256(f))
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
            os.remove(local_file)

        except (KeyboardInterrupt, Exception) as e:
            if isinstance(e, KeyboardInterrupt):
                logger.info('broken by user')
            else:
                logger.warning(pformat(e))
                logger.warning(traceback.format_exc())

if __name__ == '__main__':
    main()
