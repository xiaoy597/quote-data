# coding=utf-8

import os
import sys
import time
import shutil
import zipfile
import gzip


def decompress(zf):
    print 'Decompressing %s ...' % zf

    if zf.endswith('.gz'):
        dezf = gunzip(zf)
    else:
        dezf = unzip(zf)

    return dezf


def gunzip(zf):
    gz_file = gzip.GzipFile(zf)
    dezf = os.path.basename(zf.replace(".gz", ""))

    open(dezf, "w+").write(gz_file.read())

    gz_file.close()

    return dezf


def unzip(zf):
    zip_file = zipfile.ZipFile(zf)
    zip_file.extractall()

    files = os.listdir(os.path.join('tmp', 'backup'))

    shutil.move(os.path.join('tmp', 'backup', files[0]), files[0])

    shutil.rmtree('tmp')

    return files[0]


def gen_load_stmt(date8):

    date_iso = '%s-%s-%s' % (date8[0:4], date8[4:6], date8[6:])

    f = open('load_data.sql', 'wb')

    f.write("load data local inpath 'idx_quote.dat' "
            "overwrite into table sdata.sse_idx_quote partition (traddate='%s');\n" % date_iso)
    f.write("load data local inpath 'sec_quote.dat' "
            "overwrite into table sdata.sse_sec_quote partition (traddate='%s');\n" % date_iso)

    f.close()


if __name__ == '__main__':

    data_dir = sys.argv[1]
    cur_date = os.path.basename(data_dir)
    work_dir = os.path.join(data_dir, 'work')

    zip_files = sorted(filter(lambda x: x.startswith('mktdt00') and (x.endswith('.gz') or x.endswith('.zip')),
                              os.listdir(data_dir)))

    quote_map = {}

    if not os.path.exists(work_dir):
        os.mkdir(work_dir)

    os.chdir(work_dir)

    idx_f = open('idx_quote.dat', 'wb')
    sec_f = open('sec_quote.dat', 'wb')

    for zf in zip_files:
        dzf = decompress(os.path.join(data_dir, zf))
        hqfile = open(dzf, "r")

        lines = hqfile.readlines()

        hqfile.close()
        os.unlink(dzf)

        for line in lines:

            line = line.replace(' ', '')
            fields = line.split('|')

            if fields[0] == 'HEADER' or fields[0] == 'TRAILER':
                continue

            sec_id = fields[1]

            if fields[0] == 'MD001':
                timestamp = fields[12]
            elif fields[0] == 'MD002' or fields[0] == 'MD003':
                timestamp = fields[32]
                line = '|'.join(fields[0:31]) + '|0.0|0.0|' + '|'.join(fields[31:])
            else:
                timestamp = fields[34]

            if sec_id in quote_map:
                if quote_map[sec_id]['ts'] == timestamp:
                    continue
                else:
                    quote_map[sec_id]['ts'] = timestamp
            else:
                quote_map[sec_id] = {'s_id': fields[0], 'ts': timestamp}

            if quote_map[sec_id]['s_id'] == 'MD001':
                idx_f.write(line.decode('gbk').encode('utf8'))
            else:
                sec_f.write(line.decode('gbk').encode('utf8'))

    idx_f.close()
    sec_f.close()

    gen_load_stmt(cur_date)

