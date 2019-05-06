# GUEN Linux modules

## Description
The file unpack_archive.py is the core to extract archive files.

The file sqs_consume_publish.py is the engine to consume msg in the SQS input
queue, and upload extracted children files to the bucket/\_extracted folder.

## System Requirements
  + Ubuntu 14.04+

## Depedencies:
  * Python 3.5+  Ubuntu Packages
  * PIP Python Package
      * boto3
      * humanfriedly
      * python-magic
  * Ubuntu Packages
      * p7zip
      * gzip
      * rpm2cpio
      * cpio
      * bzip2
      * tar
      * xz-utils
      * arj
      * rzip
      * lzop
      * lzip
      * openjdk-7-jdk
      * squashfs-tools


## How to install Dependencies:
  + Ubuntu Packages: `sudo apt-get install {Ubuntu Package}`
  + Python Packages: `sudo -H pip3 install -r requirements.txt`
