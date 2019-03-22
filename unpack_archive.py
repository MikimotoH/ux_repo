import os
import shutil
import re
import logging
import zipfile
import tempfile
import subprocess
from subprocess import DEVNULL
from os.path import basename, dirname, splitext, join as pjoin, islink, isdir

logger = logging.getLogger(__name__)


class NotSupportedFileType(Exception):
    def __init__(self, ftype: str):
        self.ftype = ftype


def get_sha1(fname: str) -> str:
    import hashlib
    with open(fname, 'rb') as fin:
        return hashlib.sha1(fin.read()).hexdigest()


def detect_filetype(filepath: str) -> str:
    import magic
    desc = magic.from_file(filepath)
    if 'Zip' in desc:
        try:
            with zipfile.ZipFile(filepath) as fh:
                if 'META-INF/MANIFEST.MF' in fh.namelist():
                    return 'Jar file'
                else:
                    return desc
        except:
            return desc
    elif desc == 'data':
        with open(filepath, 'rb') as fh:
            header = fh.read(1000)
            if len(header) == 1000:
                if header.startswith(b'#!/bin/bash') or \
                        header.startswith(b'#!/bin/sh'):
                    return 'self-extracting installer'
        return desc
    return desc


def copy_without_symlink(srcdir: str, dstdir: str):
    subprocess.check_call("find . -type f | cpio -pamVd %(dstdir)s" % locals(),
                          shell=True, stdout=DEVNULL, stderr=subprocess.STDOUT, cwd=srcdir)


def unpack_archive(arcname: str, outdir: str):
    """

    :param arcname: archive file path in absolute path
    :param outdir: output directory in absolute path
    :rtype: Iterator[:str:`str`, :class:`str`]
    """
    assert os.path.exists(outdir) and os.path.isdir(outdir)
    ftype = detect_filetype(arcname)

    workdir = dirname(outdir)
    already_yielded = False

    def check_call(cmd, cwd=None):
        if type(cmd) is list:
            subprocess.check_call(cmd, stdout=DEVNULL, stderr=DEVNULL, cwd=cwd)
        else:
            subprocess.check_call(cmd, shell=True, stdout=DEVNULL, stderr=DEVNULL, cwd=cwd)

    with tempfile.TemporaryDirectory(suffix=".dir") as tmpdir:
        def gunzip_like_proc(fext: str, cmd: list):
            if splitext(arcname)[1] != fext:
                tmp_file = pjoin(tmpdir, basename(arcname)) + fext
            else:
                tmp_file = pjoin(tmpdir, basename(arcname))
            shutil.copy(arcname, tmp_file)
            check_call(cmd + [tmp_file])
            try:
                yield from unpack_archive(splitext(tmp_file)[0], outdir)
            except NotSupportedFileType:
                copy_without_symlink(tmpdir, outdir)
            else:
                os.remove(splitext(tmp_file)[0])

        if re.search(r'gzip compressed data,.+".+?\.iso"', ftype):
            # .iso.gz
            if splitext(arcname)[1] != '.gz':
                tmpfile = pjoin(tmpdir, basename(arcname)) + '.gz'
            else:
                tmpfile = pjoin(tmpdir, basename(arcname))
            shutil.copy(arcname, tmpfile)
            check_call(['gunzip', '-f', tmpfile])
            assert 'ISO 9660 CD-ROM' in detect_filetype(splitext(tmpfile)[0])
            check_call("7z x -o'%s' '%s'" % (tmpdir, splitext(tmpfile)[0]))
            os.remove(splitext(tmpfile)[0])
            copy_without_symlink(tmpdir, outdir)
        elif ftype.strip().startswith('RPM '):
            check_call("rpm2cpio '%(arcname)s' | cpio -idmv" % locals(), tmpdir)
            copy_without_symlink(tmpdir, outdir)
        elif 'Debian binary package' in ftype:
            check_call(["dpkg", "-x", arcname, tmpdir])
            copy_without_symlink(tmpdir, outdir)
        elif 'bzip2' in ftype:
            # could be tar.bz2 or .bz2
            try:
                check_call(["tar", "xvjf", arcname, "-C", tmpdir])
            except subprocess.CalledProcessError:
                # handle .bz2, not tar.bz2
                yield from gunzip_like_proc('.bz2', ["bzip2", "-d"])
            else:
                if not os.listdir(tmpdir):
                    yield from gunzip_like_proc('.bz2', ["bzip2", "-d"])
                else:
                    copy_without_symlink(tmpdir, outdir)
        elif 'XZ compressed' in ftype:
            # LZMA is also recognized as XZ
            # could be .tar.xz or .xz
            try:
                check_call(["tar", "Jxvf", arcname, "-C", tmpdir])
            except subprocess.CalledProcessError:
                # handle .xz , not .tar.xz
                yield from gunzip_like_proc('.xz', ["xz", "-d"])
            else:
                if not os.listdir(tmpdir):
                    yield from gunzip_like_proc('.xz', ["xz", "-d"])
                else:
                    copy_without_symlink(tmpdir, outdir)
        elif 'ARJ archive data' in ftype:
            if splitext(arcname)[1] != '.arj':
                tmpfile = pjoin(tmpdir, basename(arcname)) + '.arj'
            else:
                tmpfile = pjoin(tmpdir, basename(arcname))
            shutil.copy(arcname, tmpfile)
            check_call(["arj", "x", "-y", tmpfile], tmpdir)
            os.remove(tmpfile)
            copy_without_symlink(tmpdir, outdir)
        elif 'rzip compressed' in ftype:
            if splitext(arcname)[1] != '.rz':
                tmpfile = pjoin(tmpdir, basename(arcname)) + '.rz'
            else:
                tmpfile = pjoin(tmpdir, basename(arcname))
            shutil.copy(arcname, tmpfile)
            check_call(["rzip", "-d", tmpfile])
            yield from unpack_archive(splitext(tmpfile)[0], outdir)
            os.remove(splitext(tmpfile)[0])
            already_yielded = True
        elif 'lzop compressed' in ftype:
            check_call("lzop -dc '%(arcname)s'|tar xvf - -C '%(tmpdir)s'"
                       % locals())
            copy_without_symlink(tmpdir, outdir)
        elif 'lzip compressed' in ftype:
            # .lz
            if splitext(arcname)[1] != '.lz':
                tmpfile = pjoin(tmpdir, basename(arcname)) + '.lz'
            else:
                tmpfile = pjoin(tmpdir, basename(arcname))
            shutil.copy(arcname, tmpfile)
            check_call(["tar", "--use-compress-program=lzip", "-xvf",
                        tmpfile, "-C", tmpdir])
            os.remove(tmpfile)
            copy_without_symlink(tmpdir, outdir)
        elif 'gzip compressed data' in ftype:
            # .gz or .z
            try:
                check_call(["tar", "zxvf", arcname, "-C", tmpdir])
                if not os.listdir(tmpdir):
                    yield from gunzip_like_proc('.gz', ["gunzip", "-f"])
                else:
                    copy_without_symlink(tmpdir, outdir)
            except subprocess.CalledProcessError:
                logger.info('single one gzip file %s ' % arcname)
                check_call("gzip -c -d  '%s' > '%s'" % (arcname, pjoin(outdir, basename(arcname))))
        elif 'POSIX tar' in ftype:
            check_call(["tar", "xvf", arcname, "-C", tmpdir])
            copy_without_symlink(tmpdir, outdir)
        elif 'Zip' in ftype:
            try:
                check_call(["unzip", arcname, "-d", tmpdir])
            except subprocess.CalledProcessError:
                logger.info('End-of-central-directory signature not found: %s' % arcname)
                try:
                    check_call(["jar", "xvf", arcname], tmpdir)
                except BaseException as ex:
                    logger.warning('Zip file "%s" decompress failed: %s' % (arcname, ex))
            copy_without_symlink(tmpdir, outdir)
        elif 'Jar' in ftype:
            shutil.copy(arcname, pjoin(outdir, basename(arcname) + '.jar'))
        else:
            raise NotSupportedFileType(ftype)

    if not already_yielded:
        for root, _, files in os.walk(outdir):
            for f in files:
                fpath = pjoin(root, f)
                if not isdir(fpath) and not islink(fpath):
                    yield os.path.relpath(fpath, outdir), \
                          get_sha1(fpath)
    already_yielded = True
