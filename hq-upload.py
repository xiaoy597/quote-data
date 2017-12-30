# coding=utf-8

import paramiko
import os
import sys
import time
import shutil
import zipfile


def sftp_upload(host, port, username, password, local, remote):
    print '%s:  Connecting to %s ...' % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), host)
    sf = paramiko.Transport((host, port))
    sf.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(sf)
    now = time.time()

    try:
        if os.path.isdir(local):  # 判断本地参数是目录还是文件
            for f in os.listdir(local):  # 遍历本地目录
                # Don't upload files modified/created within 60 seconds.
                if os.stat(os.path.join(local, f)).st_mtime > now - 60:
                    print 'Skipping file %s ...' % os.path.join(local, f)
                    continue

                shutil.move(os.path.join(local, f), os.path.join(backup, f))

                zip_handle = zipfile.ZipFile(os.path.join(backup, f + '.zip'), 'w', zipfile.ZIP_DEFLATED)
                zip_handle.write(os.path.join(backup, f))
                print '%s is compressed.' % os.path.join(backup, f)
                zip_handle.close()
                os.remove(os.path.join(backup, f))

                print 'Uploading %s to %s ...' % (os.path.join(backup, f + '.zip'), os.path.join(remote + '/' + f + '.zip'))
                sftp.put(os.path.join(backup, f + '.zip'), os.path.join(remote + '/' + f + '.zip'))  # 上传目录中的文件
                os.remove(os.path.join(backup, f + '.zip'))

        else:
            sftp.put(local, remote)  # 上传文件
    except Exception, e:
        print('upload exception:', e)
    sf.close()


def sftp_download(host, port, username, password, local, remote):
    sf = paramiko.Transport((host, port))
    sf.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(sf)
    try:
        if os.path.isdir(local):  # 判断本地参数是目录还是文件
            for f in sftp.listdir(remote):  # 遍历远程目录
                sftp.get(os.path.join(remote, f), os.path.join(local, f))  # 下载目录中文件
        else:
            sftp.get(remote, local)  # 下载文件
    except Exception, e:
        print('download exception:', e)
    sf.close()


def create_remote_dir(host, username, password, path):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh.connect(host, username=username, password=password, timeout=300)

    print '%s: Checking remote directory %s ...' % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()), path)
    cmd = 'ls %s' % path
    (stdin, stdout, stderr) = ssh.exec_command(cmd)
    err = stderr.readline()
    if len(err.strip()) > 0:
        print err

        print 'Creating remote directory %s ...' % path
        cmd = 'mkdir %s' % path
        (stdin, stdout, stderr) = ssh.exec_command(cmd)

        err = stderr.readline()
        if len(err.strip()) > 0:
            print err

    ssh.close()


if __name__ == '__main__':
    host = sys.argv[1]  # 主机
    port = 22  # 端口
    username = 'root'  # 用户名
    password = 'root123'  # 密码
    local = sys.argv[2]  # 本地文件或目录
    remote = sys.argv[3]  # 远程文件或目录
    backup = sys.argv[4]  # 文件备份目录

    while True:
        date = time.strftime('%Y%m%d', time.localtime())

        try:
            create_remote_dir(host, username, password, remote + '/' + date)
            sftp_upload(host, port, username, password, local, remote + '/' + date)  # 上传
        except Exception, e:
            print('Exception', e)

        time.sleep(300)
