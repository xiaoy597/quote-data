# coding=utf-8

import os
import sys
from dbfread import DBF
from myutils import decompress
import multiprocessing as mp


def readDBF(dbf, sec_map, sec_f):
    print 'Reading rows from DBF %s ...' % dbf

    rows = list(DBF(dbf, encoding='gbk'))

    if len(rows) < 2:
        return

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


def gen_load_stmt(work_dir, date8):
    date_iso = '%s-%s-%s' % (date8[0:4], date8[4:6], date8[6:])

    f = open(os.path.join(work_dir, 'load_data.sql'), 'wb')

    f.write("load data local inpath 'sz_sec_quote.dat' "
            "overwrite into table sdata.szse_sec_quote partition (traddate='%s');\n" % date_iso)

    f.close()


def prepare_sz_quote(path):
    print 'Preparing SZ quote data in %s ...' % path

    data_dir = path
    cur_date = os.path.basename(data_dir)
    work_dir = os.path.join(data_dir, 'work-sz')

    zip_files = sorted(filter(lambda x: x.startswith('sjshq') and (x.endswith('.gz') or x.endswith('.zip')),
                              os.listdir(data_dir)))

    sec_map = {}

    if not os.path.exists(work_dir):
        os.mkdir(work_dir)

    sec_f = open(os.path.join(work_dir, 'sz_sec_quote.dat'), 'wb')

    for zf in zip_files:
        dzf = decompress(os.path.join(data_dir, zf), work_dir)
        readDBF(os.path.join(work_dir, dzf), sec_map, sec_f)
        os.unlink(os.path.join(work_dir, dzf))

    sec_f.close()

    gen_load_stmt(work_dir, cur_date)

    print 'Finished.'


def prepare_quote_data(root_path, from_date, to_date):
    pool = mp.Pool()

    date_list = []
    for date_path in os.listdir(root_path):
        if not os.path.isdir(os.path.join(root_path, date_path)) \
                or date_path < from_date or date_path > to_date:
            continue
        date_list.append(os.path.join(root_path, date_path))

    pool.map(prepare_sz_quote, date_list)


if __name__ == '__main__':

    if os.name != 'nt':
        sys.path.append(os.path.join(os.environ['HOME'], 'bin'))

    if len(sys.argv) < 3:
        print 'Usage: %s <data-path> <start-date> <end-date>' % sys.argv[0]
        exit(1)
    else:
        data_path = sys.argv[1]
        start_date = sys.argv[2]
        if len(sys.argv) > 3:
            end_date = sys.argv[3]
            prepare_quote_data(data_path, start_date, end_date)
        else:
            prepare_sz_quote(os.path.join(data_path, start_date))