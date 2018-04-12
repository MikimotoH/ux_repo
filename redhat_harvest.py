#!/usr/bin/env python3
# -*- coding: utf8 -*-
import re
import os
from os.path import join
import sys
from urllib.parse import urlsplit
import pdb
import traceback
import ftputil
from ftputil import FTPHost
from retrying import retry
from boto3x import upload_file


dl_dir = 'downloads'


def retry_if_ftp_error(ex):
    return isinstance(ex, (ftputil.error.FTPOSError, ConnectionResetError))


@retry(retry_on_exception=retry_if_ftp_error)
def download_file(ftp_url):
    hostname = urlsplit(ftp_url).hostname
    local_f = join(dl_dir, os.path.basename(urlsplit(ftp_url).path))
    with FTPHost(hostname, 'anonymous', 'john@gmail.com', timeout=20) as host:
        for attempt in range(100): # pylint: disable=unused-variable
            try:
                host.download(urlsplit(ftp_url).path, local_f)
                return
            except OSError:
                host = FTPHost(hostname, 'anonymous', 'john@gmail.com', timeout=20) # noqa
            except Exception as ex:
                pdb.set_trace()
                print(ex)
                traceback.print_exc()
                try:
                    os.remove(local_f)
                except:
                    pass


@retry(retry_on_exception=retry_if_ftp_error)
def crawl_ftp(repo, start=0):
    if start == 0:
        with open('redhat_list.txt', 'w') as fout:
            pass
        hostname = urlsplit(repo).hostname
        with FTPHost(hostname, 'anonymous', 'john@gmail.com', timeout=20) as host: # noqa E501
            host.keep_alive()
            pardir = urlsplit(repo).path.split('$', 1)[0]
            host.chdir(pardir)
            dirs = host.listdir('.')
            for dir in dirs: # pylint: disable=redefined-builtin
                if not re.match(r'\d+(\.\d+)*\w*', dir):
                    continue
                host.chdir(host.path.join(pardir, dir))
                subdir = repo.split('$', 1)[1].split('/', 1)[1].strip('/')
                try:
                    host.chdir(subdir)
                except ftputil.error.PermanentError:
                    subdir = subdir.rsplit('/', 1)[0]
                    try:
                        host.chdir(subdir)
                    except ftputil.error.PermanentError as ex:
                        pdb.set_trace()
                        traceback.print_exc()

                print('collect rpm files from ', host.getcwd())
                collected = 0
                with open('redhat_list.txt', 'a') as fout:
                    for root, dirs, files in host.walk(host.getcwd()):
                        for f in files:
                            if f.endswith('.rpm'):
                                remo_url = 'ftp://' + hostname + join(root, f)
                                fout.write(remo_url + '\n')
                                collected += 1
                print('Done. collected=%d' % collected)

    file_downed = 0
    with open('redhat_list.txt', 'r') as fin:
        for line in fin:
            ftp_url = line.strip()
            if not ftp_url:
                continue
            file_downed += 1
            if file_downed < start:
                continue
            print('%d, download' % file_downed, ftp_url)
            try:
                download_file(ftp_url)
            except Exception as ex:
                pdb.set_trace()
                print(ex)
                traceback.print_exc()
            local_f = join(dl_dir, os.path.basename(urlsplit(ftp_url).path)) # noqa
            print('%d, upload' % file_downed, local_f)
            try:
                upload_file(local_f)
            except Exception as ex:
                pdb.set_trace()
                print(ex)
                traceback.print_exc()

    with open('redhat_list.txt', 'r') as fin:
        all_files = sum(1 for line in fin)
    if file_downed != all_files:
        print('early loop break down. all_files=%d, file_downed=%d' %
              (all_files, file_downed))
        pdb.set_trace()
    print('file_downed=', file_downed)


def main():
    startRepo = 0
    startFile = 0
    if len(sys.argv) > 2:
        startRepo = int(sys.argv[1])
        startFile = int(sys.argv[2])
    repos = []
    os.makedirs(dl_dir, exist_ok=True)
    with open('fim_linux_repository.csv', 'r') as f:
        next(f); next(f) # pylint: disable=multiple-statements
        for l in f:
            url, distro, accessible, http_downloadable, ftp_downloadable = l.split(',') # pylint: disable=unused-variable
            url = url.strip()
            if not url.startswith('ftp://ftp.redhat.com/'):
                continue
            ftp_downloadable = ftp_downloadable.strip()
            if ftp_downloadable != 'Yes':
                continue
            repos += [url]
    for irepo, repo in enumerate(repos):
        if irepo < startRepo:
            continue
        print('[%d]repo=%s' % (irepo, repo))
        crawl_ftp(repo, startFile)
        startFile = 0


if __name__ == '__main__':
    main()
