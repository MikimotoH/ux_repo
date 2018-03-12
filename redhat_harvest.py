#!/usr/bin/env python3
# -*- coding: utf8 -*-
import re
import os
from urllib.parse import urlsplit
import ftputil
from ftputil import FTPHost
from retrying import retry
from os.path import join
import concurrent.futures


dl_dir = 'downloads'


def retry_if_ftp_error(ex):
    return isinstance(ex, ftputil.error.FTPOSError) or \
        isinstance(ex, ConnectionResetError)


@retry(retry_on_exception=retry_if_ftp_error)
def upload_file(fname):
    from ftp_credentials import ftpurl, ftpid, ftppw
    remote_fname = os.path.basename(fname)
    with FTPHost(ftpurl, ftpid, ftppw, timeout=20) as host:
        host.upload(fname, remote_fname)
    try:
        os.remove(fname)
    except:
        pass


@retry(retry_on_exception=retry_if_ftp_error)
def crawl_ftp(repo):
    file_count = 0
    hostname = urlsplit(repo).hostname
    host = FTPHost(hostname, 'anonymous', 'john@gmail.com', timeout=20)
    host.keep_alive()
    pardir = urlsplit(repo).path.split('$', 1)[0]
    host.chdir(pardir)
    dirs = host.listdir('.')
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for dir in dirs:
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
                    import pdb
                    pdb.set_trace()
                    import traceback
                    traceback.print_tb()

            for root, dirs, files in host.walk(host.getcwd()):
                for f in files:
                    if f.endswith('.rpm'):
                        remo_url = 'ftp://' + hostname + \
                            join(root, f)
                        file_count += 1
                        print('%d, download' % file_count, remo_url)
                        local_f = join(dl_dir, os.path.basename(f))
                        for attempt in range(1000):
                            try:
                                host.download(join(root, f), local_f)
                                break
                            except OSError:
                                host = FTPHost(hostname, 'anonymous', 'john@gmail.com', timeout=20) # noqa
                                pass
                            except Exception as ex:
                                import traceback
                                traceback.print_stack()
                                import pdb
                                pdb.set_trace()
                                print(ex)
                                try:
                                    os.remove(local_f)
                                except:
                                    pass
                    print('%d, upload' % file_count, local_f)
                    executor.submit(upload_file, local_f)


def main():
    repos = []
    os.makedirs(dl_dir, exist_ok=True)
    with open('fim_linux_repository.csv', 'r') as f:
        next(f); next(f) # noqa E502 E702
        for l in f:
            url, distro, accessible, http_downloadable, ftp_downloadable = \
                l.split(',')
            url = url.strip()
            if not url.startswith('ftp://ftp.redhat.com/'):
                continue
            ftp_downloadable = ftp_downloadable.strip()
            if ftp_downloadable != 'Yes':
                continue
            repos += [url]
    for repo in repos:
        print('repo=', repo)
        crawl_ftp(repo)


if __name__ == '__main__':
    main()
