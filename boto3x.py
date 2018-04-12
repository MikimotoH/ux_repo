#!/usr/bin/env python3
# coding: utf-8
import os
import re
import boto3


def change_fname(fname, fname_dup):
    ftitle, fext = os.path.splitext(fname)
    m = re.match(r'(.+)\((\d+)\)$', ftitle)
    if not m:
        return ftitle + '(%d)' % fname_dup + fext, fname_dup+1
    fname_dup = int(m.group(2))
    fname_dup += 1
    return m.group(1) + '(%d)' % fname_dup + fext, fname_dup+1


def upload_file(local_f):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('grid-harvest-uploads')
    fname = os.path.basename(local_f)
    def obj_exists(fname):
        objs = list(bucket.objects.filter(Prefix=fname))
        return len(objs) > 0 and objs[0].key == fname

    fname_dup = 1
    while obj_exists(fname):
        fname, fname_dup = change_fname(fname, fname_dup)
    bucket.upload_file(local_f, fname)
