# coding=utf-8

import shutil
import time
import sys
import os.path

source_file_list = {
    r'c:\tmp\mktdt00.txt': 0,
    r'c:\tmp\sjshq.dbf': 0,
    r'c:\tmp\SJSPHHQ.dbf': 0,
    r'c:\tmp\SJSXX.dbf': 0,
    r'c:\tmp\sjsxxn.dbf': 0,
    r'c:\tmp\SJSZHHQ.dbf': 0,
    r'c:\tmp\SJSZS.dbf': 0,
}

target_dir = sys.argv[1]

while True:
    (tm_year, tm_mon, tm_mday, tm_hour, tm_min, tm_sec, tm_wday, tm_yday, tm_isdst) = time.localtime()

    market_start_time1 = time.mktime((tm_year, tm_mon, tm_mday, 9, 14, 30, tm_wday, tm_yday, tm_isdst))
    market_end_time1 = time.mktime((tm_year, tm_mon, tm_mday, 11, 31, 30, tm_wday, tm_yday, tm_isdst))
    market_start_time2 = time.mktime((tm_year, tm_mon, tm_mday, 12, 59, 30, tm_wday, tm_yday, tm_isdst))
    market_end_time2 = time.mktime((tm_year, tm_mon, tm_mday, 15, 01, 30, tm_wday, tm_yday, tm_isdst))

    if tm_wday < 5 \
            and market_start_time1 < time.time() < market_end_time1 \
            or market_start_time2 < time.time() < market_end_time2:
        for source_file in source_file_list.keys():
            target_file = os.path.join(target_dir, os.path.basename(source_file))
            if os.path.exists(source_file):
                new_modify_time = os.stat(source_file).st_mtime
                now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                if new_modify_time > source_file_list[source_file]:
                    new_target_file = target_file + '.' + time.strftime('%Y%m%dT%H%M%S', time.localtime(new_modify_time))
                    print '%s: Copy %s to %s ...' % (now, source_file, new_target_file)
                    shutil.copy(source_file, new_target_file)
                    source_file_list[source_file] = new_modify_time
                else:
                    print '%s: File %s is not changed.' % (now, source_file)
            else:
                print '%s: File %s is not available.' % \
                      (source_file, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))

        time.sleep(1)
    else:
        print '%s: Market closed.' % time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        time.sleep(10)
