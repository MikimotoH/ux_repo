import os
import shutil
import re
import logging
import traceback
import zipfile
import tempfile
import subprocess
from subprocess import DEVNULL
from os.path import basename, splitext, join as pjoin, islink, isdir, abspath

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


def chown_to_me(pathdir: str):
    import pwd, grp
    myname = pwd.getpwuid(os.getuid())[0]
    mygroup = grp.getgrgid(os.getgid())[0]
    subprocess.check_call('sudo chown -R %(mygroup)s:%(myname)s %(pathdir)s' % locals(),
            shell=True)
    subprocess.check_call('sudo chmod a+wrx -R %(pathdir)s' % locals(), 
            shell=True)


def copy_without_symlink(srcdir: str, dstdir: str):
    chown_to_me(srcdir)
    subprocess.check_call("find . -type f | sudo cpio -pamd '%s'" % abspath(dstdir),
            shell=True, cwd=srcdir, stdout=DEVNULL, stderr=DEVNULL)
    chown_to_me(dstdir)


def check_call(cmd, cwd=None):
    if type(cmd) is list:
        subprocess.check_call(cmd, stdout=DEVNULL, stderr=DEVNULL,
                cwd=cwd)
    else:
        subprocess.check_call(cmd, shell=True, stdout=subprocess.DEVNULL,
                stderr=DEVNULL, cwd=cwd)


def unpack_archive(arcname: str, outdir: str):
    """
    decompress Linux Archive file
    :param arcname: archive file path in absolute path
    :param outdir: output directory in absolute path
    :rtype: Iterator[:str:`str`, :class:`str`]
    """
    assert os.path.exists(outdir) and os.path.isdir(outdir)
    ftype = detect_filetype(arcname)
    already_yielded = False

    try:
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
                try:
                    subprocess.check_call("rpm2cpio %s | sudo cpio -idm" % abspath(arcname),
                            shell=True, cwd=tmpdir, stdout=DEVNULL, stderr=DEVNULL)
                    chown_to_me(tmpdir)
                except subprocess.CalledProcessError as e:
                    logger.warning("extract RPM '%s' failed %s" % (arcname, e))
                    logger.warning(traceback.format_exc())
                    raise NotSupportedFileType("Failed to extract RPM '%s'" % arcname)
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
            elif 'LZMA compressed data, streamed' in ftype:
                os.rename(arcname, arcname + '.lzma')
                try:
                    subprocess.check_call(
                            "unlzma --stdout %s|cpio -idm " % abspath(arcname+'.lzma'),
                            shell=True, cwd=tmpdir)
                except subprocess.CalledProcessError as e:
                    logger.warning("extract lzma '%s' failed %s" % (arcname+'.lzma', e))
                    logger.warning(traceback.format_exc())
                    raise NotSupportedFileType("Failed to unlzma: " + arcname)
                os.rename(arcname + '.lzma', arcname)
                chown_to_me(tmpdir)
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
                        check_call(["jar", "xvf", abspath(arcname)], tmpdir)
                    except BaseException as ex:
                        logger.warning('Zip file "%s" decompress failed: %s' % (arcname, ex))
                copy_without_symlink(tmpdir, outdir)
            elif 'Jar' in ftype:
                shutil.copy(arcname, pjoin(outdir, basename(arcname) + '.jar'))
            elif 'Squashfs filesystem' in ftype:
                try:
                    subprocess.check_call("sudo unsquashfs %s" % abspath(arcname),
                            shell=True, cwd=tmpdir)
                except subprocess.CalledProcessError as e:
                    logger.warning("unsquashfs failed: " + arcname)
                    logger.warning(traceback.format_exc())
                    raise NotSupportedFileType("unsquashfs failed :" + arcname)
                chown_to_me(tmpdir)
                copy_without_symlink(tmpdir, outdir)
            else:
                raise NotSupportedFileType(ftype)
    except PermissionError as e:
        try:
            chown_to_me(tmpdir)
            shutil.rmtree(tmpdir)
        except subprocess.SubprocessError as e2:
            logger.warning('failed to chown_to_me %s %s' % (tmpdir, e2))
        logger.warning(str(e))
        logger.warning(traceback.format_exc())
        logger.warning('failed to remove tmpdir= ' + tmpdir)

    if not already_yielded:
        for root, _, files in os.walk(outdir):
            for f in files:
                fpath = pjoin(root, f)
                if not isdir(fpath) and not islink(fpath):
                    yield os.path.relpath(fpath, outdir), get_sha1(fpath)
