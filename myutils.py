# coding=utf-8

import os
import shutil
import zipfile
import gzip


def decompress(zf, work_dir):
    print 'Decompressing %s ...' % zf

    if zf.endswith('.gz'):
        dezf = gunzip(zf, work_dir)
    else:
        dezf = unzip(zf, work_dir)

    return dezf


def gunzip(zf, work_dir):
    cur_dir = os.path.curdir
    os.chdir(work_dir)

    gz_file = gzip.GzipFile(zf)
    dezf = os.path.basename(zf.replace(".gz", ""))

    open(dezf, "w+").write(gz_file.read())

    gz_file.close()

    os.chdir(cur_dir)

    return dezf


def unzip(zf, work_dir):

    cur_dir = os.path.curdir
    os.chdir(work_dir)

    zip_file = zipfile.ZipFile(zf)
    zip_file.extractall()

    files = os.listdir(os.path.join('tmp', 'backup'))

    shutil.move(os.path.join('tmp', 'backup', files[0]), files[0])

    shutil.rmtree('tmp')

    os.chdir(cur_dir)

    return files[0]

