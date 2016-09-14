#!/usr/bin/python
#encoding=utf-8
from ftplib import FTP
import sys
import os.path

class FTPClient(object):
    def __init__(self, host, port, user, password):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self.__connect_host()
    def __connect_host(self):
        self._ftp = FTP()
        self._ftp.connect(self._host, self._port)
        self._ftp.login(self._user, self._password)
    def download(self, remote_path, local_path):
        remote_dir = os.path.dirname(remote_path)
        remote_file = os.path.basename(remote_path)
        local_file = '/' + local_path.strip('/') + '/' + remote_file
        bufsize = 16
        self._ftp.cwd(remote_dir)
        self._ftp.voidcmd('TYPE I')
        file_size = self._ftp.size(remote_file)
        local_size = 0
        if os.path.exists(local_file):
            local_size = os.stat(local_file).st_size
        if local_size >= file_size:
            print("local file is bigger or equal remote file")
        conn = self._ftp.transfercmd('RETR %s' % remote_file, local_size)
        local_f = open(local_file, 'ab')
        trans_size = local_size
        while True:
            data = conn.recv(bufsize)
            if not data:
                break
            local_f.write(data)
            trans_size += len(data)
            #print('\rdownload process: %.2f%%' % (float(trans_size) / file_size * 100))
        local_f.close()
        conn.close()
        self._ftp.quit()
    

    def upload(self, local_path, remote_path):
        remote_file = os.path.basename(local_path)
        bufsize = 1024
        self._ftp.cwd(remote_path)
        self._ftp.voidcmd('TYPE I')
        file_size = os.stat(local_path).st_size
        try:
            remote_size = self._ftp.size(remote_file)
        except:
            remote_size = 0
        if remote_size >= file_size:
            print("remote file is bigger or equal than local file")
            exit(0)
        local_f = open(local_path,'rb')
        local_f.seek(remote_size)
        conn, _ = self._ftp.ntransfercmd('STOR %s' % remote_file, remote_size)
        trans_size = remote_size
        while True:
            data = local_f.read(bufsize)
            if not data:
                break
            conn.sendall(data)
            trans_size += len(data)
            #print('\rupload process: %.2f%%' % (float(trans_size) / file_size * 100))
        conn.close()
        local_f.close()




if __name__ == '__main__':
    #connect = FTPClient('127.0.0.1', 21, 'cn01', 'airation')
    #connect.download('/home/cn01/Downloads/ubuntu-14.04.4-desktop-amd64.iso', '/home/cn01/wyc/test')
    #connect.upload('/home/cn01/Downloads/ubuntu-14.04.4-desktop-amd64.iso', '/home/cn01/wyc/test')
    connect =  FTPClient('cn06', 21, 'cn06', 'airation')
    #connect.download('/home/cn06/soft/wyc.tar', '/home/cn01/wyc/test')
    connect.upload('/home/cn01/wyc/test/wyc.tar', '/home/cn06/soft/test')
