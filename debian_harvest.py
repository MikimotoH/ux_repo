#!/usr/bin/env python3
# coding: utf-8
import os
import urllib
from pyquery import PyQuery as pq
from boto3x import upload_file


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
    os.makedirs(dl_dir, exist_ok=True)
    for line_num, f_url in enumerate(recursion(harvest_url)):
        print('%d download ' % line_num, f_url)
        download_file(f_url)


if __name__ == '__main__':
    main()
