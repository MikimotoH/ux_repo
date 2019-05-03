#!/usr/bin/python3.6
# -*- coding: utf8 -*-
import re
import os
import logging
import traceback
import sys
import asyncio
import shutil
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError
from urllib.parse import urlparse
from pprint import pformat
from pyquery import PyQuery as pq
from boto3x import upload_file
from typing import Generator, List


dl_dir = 'downloads'
logger = logging.getLogger(__name__)
executor = ThreadPoolExecutor(os.cpu_count()*2)


def repo_to_regex(repo: str) -> str:
    releasever = r'\d+(\.\d+)*'
    basearch = r'\w+'
    r = re.escape(repo)
    r = r.replace(r'\$releasever', releasever)
    r = r.replace(r'\$basearch', basearch)
    r = r + '.*'
    return r


def recursion(url: str) -> Generator[str, None, str]:
    try:
        logger.info('Visit ' + url)
        d = pq(url=url, verify=False)
    except HTTPError as e:
        logger.info("Failed to visit %s  reason: %s" % (url, e.reason))
        return ''

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


def download_file(f_url: str) -> None:
    import requests
    import shutil
    from os.path import basename, join as pjoin
    local_f = pjoin(dl_dir, basename(urlparse(f_url).path))
    try:
        r = requests.get(f_url, stream=True, timeout=60, verify=False)
    except HTTPError as e:
        logger.info('Failed to download ' + f_url)
        logger.info(e.reason)
        logger.info(traceback.format_exc())
        return

    logger.info("download " + f_url)
    lastModified = r.headers['Last-Modified']
    contentType = r.headers['Content-Type']
    with open(local_f, mode='wb') as f:
        shutil.copyfileobj(r.raw, f)
    try:
        logger.info('upload %s' % basename(local_f))
        upload_file(f_url, local_f, contentType, lastModified)
    except Exception as e:
        logger.warning('upload failed:' + f_url)
        logger.warning(str(e))
        logger.warning(traceback.format_exc())
    try:
        os.remove(local_f)
    except Exception as e:
        logger.warning('Failed to remove file: ' + local_f)
        logger.warning(str(e))
        logger.warning(traceback.format_exc())


visited_repos: List[str] = []


def harvest_csv_file(csv_file: str):
    with open(csv_file, 'r') as f:
        for l in f:
            l = l.strip()  # noqa E741
            l = l.split(',')[0]  # noqa E741
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
                    logger.info('download ' + f_url)
                    global executor
                    executor.submit(download_file, f_url)


async def main():
    try:
        if os.path.exists(dl_dir):
            shutil.rmtree(dl_dir, ignore_errors=True)
        os.makedirs(dl_dir, exist_ok=True)
        harvest_csv_file('fim_linux_repository.csv')
        harvest_csv_file('list of repositories.csv')
        global executor
        executor.shutdown(wait=True)
    except KeyboardInterrupt:
        return
    except Exception as e:
        logger.warning(pformat(e))
        traceback.print_exc()


if __name__ == '__main__':
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s %(message)s",
                        stream=sys.stdout, level=logging.INFO)
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    finally:
        loop.run_until_complete(
            loop.shutdown_asyncgens())  # see: https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.shutdown_asyncgens
        loop.close()
