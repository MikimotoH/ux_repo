#!/usr/bin/env python3
# -*- coding: utf8 -*-
import re
import os
import urllib
from pyquery import PyQuery as pq
from boto3x import upload_file


def repo_to_regex(repo):
    releasever = r'\d+(\.\d+)*'
    basearch = r'\w+'
    r = re.escape(repo)
    r = r.replace(r'\$releasever', releasever)
    r = r.replace(r'\$basearch', basearch)
    r = r + '.*'
    return r


def recursion(url):
    import time
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


def download_file(f_url):
    import requests
    import shutil
    from os.path import basename, join
    local_f = join(dl_dir, basename(urllib.parse.urlparse(f_url).path))
    for _try in range(60):
        try:
            r = requests.get(f_url, stream=True, timeout=60)
            lastModified = r.headers['Last-Modified']
            contentType = r.headers['Content-Type']
            with open(local_f, mode='wb') as f:
                shutil.copyfileobj(r.raw, f)
            try:
                print('upload', basename(local_f))
                upload_file(f_url, local_f, contentType, lastModified)
            except Exception as ex:
                print('upload "%s" failed %s' % (local_f, ex))
            try:
                os.remove(local_f)
            except:
                pass
            return
        except:
            pass
    print('download failed: ', f_url)


def main():
    visited_repos = []
    repos = []
    os.makedirs(dl_dir, exist_ok=True)

    with open('list of repositories.csv', 'r') as f:
        for l in f:
            l = l.strip()
            if not l:
                continue
            repos += [l]
    for repo in repos:
        print('repo=', repo)
        repo = repo.split('$', 1)[0]
        if repo in visited_repos:
            continue
        visited_repos += [repo]
        with open('centos_items.txt', 'w') as f:
            reporegexs = [_ for _ in repos if _.startswith(repo)]
            reporegexs = [repo_to_regex(_) for _ in reporegexs]
            for f_url in recursion(repo):
                if any(re.match(_, f_url) for _ in reporegexs):
                    f.write(f_url + '\n')
                    print('download ', f_url)
                    download_file(f_url)
        # download per centos_items.txt
        # with open('centos_items.txt', 'r') as f:
        #     for line_num, l in enumerate(f):


if __name__ == '__main__':
    main()
