# coding=utf-8

import os
import sys
from myutils import decompress
import multiprocessing as mp


def gen_load_stmt(work_dir, date8):
    date_iso = '%s-%s-%s' % (date8[0:4], date8[4:6], date8[6:])

    f = open(os.path.join(work_dir, 'load_data.sql'), 'wb')

    f.write("load data local inpath 'idx_quote.dat' "
            "overwrite into table sdata.sse_idx_quote partition (traddate='%s');\n" % date_iso)
    f.write("load data local inpath 'sec_quote.dat' "
            "overwrite into table sdata.sse_sec_quote partition (traddate='%s');\n" % date_iso)

    f.close()


def prepare_sh_quote(path):
    print 'Preparing SH quote data in %s ...' % path

    data_dir = path
    cur_date = os.path.basename(data_dir)
    work_dir = os.path.join(data_dir, 'work')

    zip_files = sorted(filter(lambda x: x.startswith('mktdt00') and (x.endswith('.gz') or x.endswith('.zip')),
                              os.listdir(data_dir)))

    quote_map = {}

    if not os.path.exists(work_dir):
        os.mkdir(work_dir)

    idx_f = open(os.path.join(work_dir, 'idx_quote.dat'), 'wb')
    sec_f = open(os.path.join(work_dir, 'sec_quote.dat'), 'wb')

    for zf in zip_files:
        dzf = decompress(os.path.join(data_dir, zf), work_dir)
        hqfile = open(os.path.join(work_dir, dzf), "r")

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

    gen_load_stmt(work_dir, cur_date)


def prepare_quote_data(root_path, from_date, to_date):
    pool = mp.Pool()

    date_list = []
    for date_path in os.listdir(root_path):
        if not os.path.isdir(os.path.join(root_path, date_path)) \
                or date_path < from_date or date_path > to_date:
            continue
        date_list.append(os.path.join(root_path, date_path))

    pool.map(prepare_sh_quote, date_list)


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
        else:
            end_date = start_date

        prepare_quote_data(data_path, start_date, end_date)
