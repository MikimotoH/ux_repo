#!/usr/bin/env python3
# -*- coding: utf8 -*-
import re
import os
import urllib
import time
import logging
import traceback
import sys
from pprint import pformat
from pyquery import PyQuery as pq
from boto3x import upload_file


logger = logging.getLogger(__name__)


def repo_to_regex(repo: str) -> str:
    releasever = r'\d+(\.\d+)*'
    basearch = r'\w+'
    r = re.escape(repo)
    r = r.replace(r'\$releasever', releasever)
    r = r.replace(r'\$basearch', basearch)
    r = r + '.*'
    return r


def recursion(url: str) -> str:
    d = pq(url=url)

    items = d('table:nth-child(6) tr td a')
    if not items:
        items = d('table:nth-child(13) tr td a')
    for item in items[1:]:
        if not item.text:
            continue
        item_url = d.base_url + item.attrib['href']
        if item_url.endswith('/'):
            yield from recursion(item_url)
        else:
            yield item_url


dl_dir = 'downloads'


def download_file(f_url: str):
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
                logger.info('upload %s' % basename(local_f))
                upload_file(f_url, local_f, contentType, lastModified)
            except Exception as ex:
                logger.warning('upload "%s" failed %s' % (local_f, pformat(ex)))
            try:
                os.remove(local_f)
            except:
                pass
            return
        except:
            pass
    logger.warning('download failed: %s' % f_url)


visited_repos = []


def harvest_csv_file(csv_file:str):
    with open(csv_file, 'r') as f:
        for l in f:
            l = l.strip()
            l = l.split(',')[0]
            if not l or not re.match(r'http://|https://', l):
                continue
            repo = l
            logger.info('repo=%s' % repo)
            reporegex = repo_to_regex(repo)
            repo = repo.split('$', 1)[0]
            global visited_repos
            if repo in visited_repos:
                continue
            visited_repos += [repo]
            for f_url in recursion(repo):
                if re.match(reporegex, f_url):
                    logger.info('download %s' % f_url)
                    download_file(f_url)


def main():
    try:
        logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s %(message)s",
                stream=sys.stdout, level=logging.INFO)
        os.makedirs(dl_dir, exist_ok=True)
        # download_file('http://debuginfo.centos.org/')
        harvest_csv_file('fim_linux_repository.csv')
        harvest_csv_file('list of repositories.csv')
    except KeyboardInterrupt:
        return
    except Exception as e:
        logger.warning(pformat(e))
        traceback.print_exc()


if __name__ == '__main__':
    main()

