#!/usr/bin/env python3
# -*- coding: utf8 -*-
from pyquery import PyQuery as pq
import os
import urllib
import sys


def recursion(url):
    for try_count in range(100):
        try:
            d = pq(url=url)
            break
        except Exception as ex:
            print('try_count=', try_count, ' for url=', url)
            if try_count == 99:
                print(ex)
                print('Failed to visit ', url)
                return
            import time
            time.sleep(0.1)

    try:
        items = d('a')
    except Exception as ex:
        print(ex)
        print('no CSS selector in pyquery d')
        return
    start = False
    for item in items:
        if item.text == 'Parent Directory' or item.text == '../':
            start = True
            continue
        if not start:
            continue
        item_url = d.base_url + item.attrib['href']
        if item_url.endswith('.deb'):
            yield item_url
        elif item_url.endswith('/'):
            yield from recursion(item_url)


dl_dir = 'downloads'
harvest_url = 'http://ftp.us.debian.org/debian/pool/'


def download_file(file_url):
    import requests
    import shutil
    fname = os.path.basename(urllib.parse.urlparse(file_url).path)
    fname = os.path.join(dl_dir, fname)
    r = requests.get(file_url, stream=True)
    with open(fname, mode='wb') as f:
        shutil.copyfileobj(r.raw, f)
    return fname


def upload_file(fname):
    import ftputil
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


def main():
    os.makedirs(dl_dir, exist_ok=True)
    for line_num, f_url in enumerate(recursion(harvest_url)):
        print('%d download ' % line_num, f_url)
        try:
            fname = download_file(f_url)
        except Exception as ex:
            print(ex)
        else:
            print('upload', fname)
            upload_file(fname)
            try:
                os.remove(fname)
            except:
                pass


if __name__ == '__main__':
    main()
