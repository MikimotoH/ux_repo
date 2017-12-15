#!/usr/bin/env python3
# -*- coding: utf8 -*-
from pyquery import PyQuery as pq
import re
import os
import urllib
import sys


def repo_to_regex(repo):
    releasever = r'\d+(\.\d+)*'
    basearch = r'\w+'
    r = re.escape(repo)
    r = r.replace(r'\$releasever', releasever)
    r = r.replace(r'\$basearch', basearch)
    r = r + '.*'
    return r


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
        items = d('table:nth-child(6) tr td a')
    except Exception as ex:
        print(ex)
        print('no CSS selector in pyquery d')
        return
    for item in items[1:]:
        if not item.text:
            continue
        item_url = d.base_url + item.attrib['href']
        if item_url.endswith('/'):
            yield from recursion(item_url)
        else:
            yield item_url


dl_dir = 'downloads'


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
    visited_repos = []
    repos = []
    with open('list of repositories.csv', 'r') as f:
        for l in f:
            l = l.strip()
            if not l:
                continue
            repos += [l]
    for repo in repos[5:]:
        print('repo=', repo)
        repo = repo.split('$', 1)[0]
        if repo in visited_repos:
            continue
        visited_repos += [repo]
        with open('centos_items.txt', 'w') as f:
            reporegexs = [_ for _ in repos if _.startswith(repo)]
            reporegexs = [repo_to_regex(_) for _ in reporegexs]
            for c_url in recursion(repo):
                if any(re.match(_, c_url) for _ in reporegexs):
                    f.write(c_url + '\n')
        # download per centos_items.txt
        os.makedirs(dl_dir, exist_ok=True)
        with open('centos_items.txt', 'r') as f:
            for line_num, l in enumerate(f):
                f_url = l.strip()
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
