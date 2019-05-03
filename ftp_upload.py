#!/usr/bin/env python3
# -*- coding: utf8 -*-
import os
import sys
from retrying import retry
import ftputil


def upload_file(fname):
    from ftp_credentials import ftpurl, ftpid, ftppw
    remote_fname = os.path.basename(fname)
    for retry_count in range(5):
        try:
            with ftputil.FTPHost(ftpurl, ftpid, ftppw, timeout=20) as host:
                host.upload(fname, remote_fname)
                return
        except ftputil.error.FTPOSError:
            print('retry %d' % retry_count, fname)
    print('Failed to upload ', fname, file=sys.stderr)


def retry_if_ftp_error(ex):
    return isinstance(ex, (ftputil.error.FTPOSError, ConnectionResetError))


@retry(retry_on_exception=retry_if_ftp_error)
def upload_file_2(fname):
    from ftp_credentials import ftpurl, ftpid, ftppw
    remote_fname = os.path.basename(fname)
    with ftputil.FTPHost(ftpurl, ftpid, ftppw, timeout=20) as host:
        host.upload(fname, remote_fname)
    try:
        os.remove(fname)
    except:
        pass
