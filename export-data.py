# coding=utf-8

import os
import sys
import datetime

if __name__ == '__main__':
    start_dt = datetime.datetime.strptime(sys.argv[1], '%Y%m%d')
    end_dt = datetime.datetime.strptime(sys.argv[2], '%Y%m%d')

    curr_dt = start_dt
    while curr_dt <= end_dt:
        print(
            "hive -e \"select * from sdata.sse_sec_quote where securityid='601318'" +
            " and traddate = '%s' order by ts\" > 601318.%s.dat" %
            (curr_dt.strftime('%Y-%m-%d'), curr_dt.strftime('%Y%m%d')))
        curr_dt += datetime.timedelta(days=1)

