# coding=utf-8

import os
import sys
import time
import shutil
import zipfile
import gzip
from dbfread import DBF


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


def readDBF(dbf):
    print 'Reading rows from DBF %s ...' % dbf

    rows = list(DBF(dbf, encoding='gbk'))

    header = rows[0]

    new_ts = header['HQCJBS']

    for row in rows[1:]:
        fields = []
        for f in row.keys():
            if type(row[f]) is unicode:
                fields.append(row[f])
            else:
                fields.append(str(row[f]))
        row_str = '|'.join(fields)
        if row['HQZQDM'] not in sec_map:
            sec_map[row['HQZQDM']] = row_str
        elif sec_map[row['HQZQDM']] != row_str:
            sec_map[row['HQZQDM']] = row_str
        else:
            continue
        # print row_str
        sec_f.write('%s|%s\n' % (row_str.encode('utf8'), new_ts))


def gen_load_stmt(date8):

    date_iso = '%s-%s-%s' % (date8[0:4], date8[4:6], date8[6:])

    f = open('load_data.sql', 'wb')

    f.write("load data local inpath 'sz_sec_quote.dat' "
            "overwrite into table sdata.szse_sec_quote partition (traddate='%s');\n" % date_iso)

    f.close()


if __name__ == '__main__':

    data_dir = sys.argv[1]
    cur_date = os.path.basename(data_dir)
    work_dir = os.path.join(data_dir, 'work-sz')

    zip_files = sorted(filter(lambda x: x.startswith('sjshq') and (x.endswith('.gz') or x.endswith('.zip')),
                              os.listdir(data_dir)))

    sec_map = {}

    if not os.path.exists(work_dir):
        os.mkdir(work_dir)

    os.chdir(work_dir)

    sec_f = open('sz_sec_quote.dat', 'wb')

    for zf in zip_files:
        dzf = decompress(os.path.join(data_dir, zf))
        readDBF(dzf)
        os.unlink(dzf)

    sec_f.close()

    gen_load_stmt(cur_date)

    print 'Finished.'
