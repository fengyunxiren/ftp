#!/usr/bin/python
#This source is encapsulates many transmission method.
#include scp, ftp, sftp and fbtftp.
#Usage: multi_trans.py [-h] [-t transport] [-p port] send_file receive_path
import sys
import paramiko
import os
import time
import string
import random
import stat
import thread
import threading
from ftplib import FTP
import socket
import struct
import logging
import argparse
import getpass

# used for fbtftp
DEFAULT_BLKSIZE = 512
OPCODE_RRQ = 1
OPCODE_DATA = 3
OPCODE_ACK = 4
OPCODE_OACK = 6
ERROR = 1
SUCCESS = 0

class FileAttribute(object):
    """
    get file size and check whether the file is exist.
    this class is used for local file.
    """
    def __init__(self):
        self._file_size = 0
        self._file_exist = False

    def getFileSize(self, path):
        self._file_size = os.stat(path).st_size
        return self._file_size

    def fileExist(self, path):
        self._file_exist = os.path.exists(path)
        return self._file_exist


class SFTPFileAttribute(FileAttribute):
    """
    get file size and check whether the file is exist.
    this class is used for remote server with sftp.
    """
    def __init__(self, sftp):
        super(SFTPFileAttribute, self).__init__()
        self.sftp = sftp

    def getFileSize(self, path):
        self._file_size = self.sftp.stat(path).st_size
        return self._file_size

    def fileExist(self, path):
        base_dir = os.path.dirname(path)
        base_name = os.path.basename(path)

        if base_name in self.sftp.listdir(base_dir):
            return True
        else:
            return False


class FTPFileAttribute(FileAttribute):
    """
    get file size and check whether the file is exist.
    this class is used for remote server with ftp.
    """
    def __init__(self, ftp):
        super(FTPFileAttribute, self).__init__()
        self.ftp = ftp

    def getFileSize(self, path):
        self._file_size = self.ftp.size(path)
        return self._file_size

    def fileExist(self, path):
        base_dir = os.path.dirname(path)
        base_name = os.path.basename(path)
        self.ftp.cwd(base_dir)
        if base_name in self.ftp.nlst():
            return True
        else:
            return False

class BaseTransport(object):
    """
    Base class, inherited by four subclasses.
    SFTPTransport, SCPTransport, FTPTransport and FBTFTBTransport.
    has two method.
    _viewBar is used for displaying progress bar.
    _progressBarShow is used for get downloading or uploading file size, and tranfer
    it to _viewBar.
    """
    def __init__(self, host, port, user, password):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._stop = False


    def _viewBar(self, count, rate, rate_units='Kb/s', get_size='', total_size='', used_time=''):
        count_percent = '\r%d%%' % count
        if count < 10:
            count_percent += '  '
        elif count <100:
            count_percent += ' '

        last_put = '%s/%sKb' % (get_size, total_size)
        last_put += '  %6.2f' % rate + rate_units
        last_put += '  %.0fs  ' % used_time

        width = int(os.popen('stty size','r').read().split()[-1])
        length_bar = width/2
        i = length_bar * count /100
        progress_bar = '[' + i * '>' + (length_bar-i) * ' ' + ']'
        white_length = width - len(count_percent) - len(last_put) - len(progress_bar)
        if white_length < 0:
            white_length += len(progress_bar)
            if white_length < 0:
                sys.stdout.write(count_percent+last_put)
                sys.stdout.flush()
                return
            white_space = white_length * ' '
            sys.stdout.write(count_percent+white_space+last_put)
            sys.stdout.flush()
            return
        white_space = white_length * ' '
        sys.stdout.write(count_percent+progress_bar+white_space+last_put)
        sys.stdout.flush()
        return



    def _progressBarShow(self, file_name, file_size, time_down_start, file_attribute=FileAttribute()):
        if file_size < 1024:
            return 0
        base_dir = os.path.dirname(file_name)
        base_name = os.path.basename(file_name)

        while not self._stop:
            rate_time = time.time()
            if file_attribute.fileExist(file_name):
                rate_size = file_attribute.getFileSize(file_name)
                initial_size = rate_size
                break
            if rate_time - time_down_start > 10:
                print("File %s not exist in %s" % (base_name, base_dir))
                exit(1)
            time.sleep(0.1)

        while not self._stop:
            file_receive_size = file_attribute.getFileSize(file_name)
            time_interval = time.time() - rate_time
            rate_time = time.time()
            count = file_receive_size / (file_size / 100)
            rate = (file_receive_size - rate_size) / time_interval / 1024
            rate_size = file_receive_size
            time_left = (file_size - file_receive_size) / 1024 / (1 if rate == 0 else rate)
            if rate > 1024:
                rate /= 1024
                self._viewBar(count, rate, 'Mb/s', file_receive_size/1024, file_size/1024, time_left)
            else:
                self._viewBar(count, rate, get_size=file_receive_size/1024, total_size=file_size/1024, used_time=time_left)
            if file_receive_size == file_size:
                break
            time.sleep(1.0)

        total_time = time.time() - time_down_start
        average_speed = (file_receive_size - initial_size) / 1024 / total_time
        count = file_receive_size / (file_size / 100)
        rate_units = 'Kb/s'
        if average_speed > 1024:
            average_speed /= 1024
            rate_units = 'Mb/s'
            self._viewBar(count, average_speed, rate_units, file_receive_size/1024, file_size/1024, total_time)
            sys.stdout.flush()
        else:
            self._viewBar(count, average_speed, get_size=file_receive_size/1024, total_size=file_size/1024, used_time=total_time)
            sys.stdout.flush()
        print("\n")



class SFTPTransport(BaseTransport):
    def __init__(self, host, port, user, password):
        super(SFTPTransport, self).__init__(host, port, user, password)
        self.__connectHost()



    def __connectHost(self):
        self._ssh = paramiko.SSHClient()
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._ssh.connect(self._host, self._port, username=self._user, password=self._password)
        self._sftp = self._ssh.open_sftp()



    def download(self, remote_path, local_path):
        if stat.S_ISDIR(self._sftp.stat(remote_path).st_mode):
            tar_name = '/tmp/' + self.__randomString(16) + '.tar'
            tar_name = str(tar_name)
            tar_dir = remote_path.strip('/').split('/')[-1]
            remote_dir = '/' + '/'.join(remote_path.strip('/').split('/')[:-1])
            self._ssh.exec_command('cd %s;tar cvf %s %s > /dev/null' % (remote_dir, tar_name, tar_dir))
            time.sleep(0.01)

            start_size = self._sftp.stat(tar_name).st_size
            start_time = time.time()
            while not self._stop:
                if (time.time() - start_time) > 3600:
                    print("The file is too big or something wrong happened")
                    exit(1)
                later_size = self._sftp.stat(tar_name).st_size
                if later_size == start_size:
                    break
                start_size = later_size
                time.sleep(0.1)

            thread.start_new_thread(self._sftp.get, (tar_name, tar_name))
            self._progressBarShow(tar_name, start_size, start_time)
            self._ssh.exec_command('rm -rf %s' % tar_name)
            if os.path.exists(tar_name):
                os.system('tar xvf %s -C %s > /dev/null' % (tar_name, local_path))
                os.system('rm -rf %s' % tar_name)
        else:
            file_name = '/' + local_path.strip('/') + '/' + os.path.basename(remote_path)
            file_size = self._sftp.stat(remote_path).st_size
            time_down_start = time.time()
            if os.path.exists(file_name):
                os.system('rm -rf %s' % file_name)

            thread.start_new_thread(self._sftp.get, (remote_path, file_name))
            self._progressBarShow(file_name, file_size, time_down_start)



    def upload(self, local_path, remote_path):
        if os.path.isdir(local_path):
            tar_name = '/tmp/'+self.__randomString(16)+'.tar'
            tar_name = str(tar_name)
            tar_dir = local_path.strip('/').split('/')[-1]
            local_dir = '/'+'/'.join(local_path.strip('/').split('/')[:-1])
            time_down_start = time.time()
            os.system('cd %s;tar cvf %s %s >/dev/null' % (local_dir, tar_name, tar_dir))
            file_size = os.stat(tar_name).st_size
            thread.start_new_thread(self._sftp.put, (tar_name, tar_name))
            new_sftp = self._ssh.open_sftp()
            file_attribute = SFTPFileAttribute(new_sftp)
            self._progressBarShow(tar_name, file_size, time_down_start, file_attribute)

            new_sftp.close()
            os.system('rm -rf %s' % tar_name)
            self._ssh.exec_command('tar xvf %s -C %s >/dev/null;rm -rf %s' % (tar_name, remote_path, tar_name))

        else:
            file_name = '/'+remote_path.strip('/')+'/'+os.path.basename(local_path)
            file_size = os.stat(local_path).st_size
            time_down_start = time.time()

            thread.start_new_thread(self._sftp.put, (local_path, file_name))
            new_sftp = self._ssh.open_sftp()
            file_attribute = SFTPFileAttribute(new_sftp)
            self._progressBarShow(file_name, file_size, time_down_start, file_attribute)
            new_sftp.close()





    def __randomString(self, num):
        return ''.join(random.sample(string.ascii_letters+string.digits, num))




class SCPTransport(BaseTransport):
    def __init__(self, host, port, user, password):
        super(SCPTransport, self).__init__(host, port, user, password)



    def download(self, remote_path, local_path):
        os.system('scp -r %s@%s:%s %s' % (self._user, self._host, remote_path, local_path))


    def upload(self, local_path, remote_path):
        os.system('scp -r %s %s@%s:%s' % (local_path, self._user, self._host, remote_path))



class FTPTransport(BaseTransport):
    def __init__(self, host, port, user, password):
        super(FTPTransport, self).__init__(host, port, user, password)
        self.__connectHost()


    def __del__(self):
        if self._ftp:
            self._ftp.close()



    def __connectHost(self):
        self._ftp = FTP()
        self._ftp.connect(self._host, self._port)
        self._ftp.login(self._user, self._password)



    def download(self, remote_path, local_path):
        remote_dir = os.path.dirname(remote_path)
        remote_file = os.path.basename(remote_path)
        local_file = '/' + local_path.strip('/') + '/' + remote_file
        bufsize = 1024
        file_list = self._ftp.nlst(remote_path)
        if len(file_list) > 1 or file_list[0] != remote_path:
            os.system('mkdir -p %s' % local_file)
            self._ftp.cwd(remote_path)
            trans_files = self._ftp.nlst()
            for trans_file in trans_files:
                trans_path = '/' + remote_path.strip('/') + '/' + trans_file
                self.download(trans_path, local_file)
        else:
            self._ftp.cwd(remote_dir)
            self._ftp.voidcmd('TYPE I')
            file_size = self._ftp.size(remote_file)
            if os.path.exists(local_file):
                local_size = os.stat(local_file).st_size
            else:
                local_size = 0
            if local_size > file_size:
                print("Warnning: The file you transfer is exist in local path, and is not match the remote file!")
                print("If you want remote file, delete the local file or change the local path!")
                return
            elif local_size == file_size:
                print("The file you transfer has already exist! so it has nonthing to do.")
                return
            conn = self._ftp.transfercmd('RETR %s' % remote_file, local_size)
            file_handler = open(local_file, 'ab')
            time_down_start = time.time()
            threading.Thread(target=self._progressBarShow, args=(local_file, file_size, time_down_start)).start()
            try:
                while True:
                    data = conn.recv(bufsize)
                    if not data:
                        break
                    file_handler.write(data)
            except:
                self._stop = True
            file_handler.close()
            conn.close()



    def upload(self, local_path, remote_path):
        if os.path.isdir(local_path):
            save_path = '/' + remote_path.strip('/') +'/' + os.path.basename(local_path)
            self._ftp.mkd(save_path)
            self._ftp.cwd(save_path)
            trans_files = os.listdir(local_path)
            for trans_file in trans_files:
                trans_path = '/' + local_path.strip('/') + '/' + trans_file
                self.upload(trans_path, save_path)
        else:
            remote_file = os.path.basename(local_path)
            bufsize = 1024
            self._ftp.cwd(remote_path)
            self._ftp.voidcmd('TYPE I')
            file_size = os.stat(local_path).st_size
            try:
                remote_size = self._ftp.size(remote_file)
            except:
                remote_size = 0

            if remote_size > file_size:
                print("Warnning: The file you transfer is exist in remote path, and is not match the local file!")
                print("If you want upload it continue, please change the remote path!")
                return
            elif remote_size == file_size:
                print("The file you transfer has already exist! so it has nonthing to do.")
                return

            file_handler = open(local_path, 'rb')
            file_handler.seek(remote_size)
            conn, _ = self._ftp.ntransfercmd('STOR %' % remote_file, remote_size)
            time_down_start = time.time()
            new_ftp = FTP()
            new_ftp.connect(self._host, self._port)
            new_ftp.login(self._user, self._password)
            file_attribute = FTPFileAttribute(new_ftp)
            thread.start_new_thread(self._progressBarShow, (file_name, file_size, time_down_start, file_attribute))
            while True:
                data = file_handler.read(bufsize)
                if not data:
                    break
                conn.sendall(data)

            conn.close()
            file_handler.close()
            
            new_ftp.quit()



class FBTFTPTransport(BaseTransport):
    def __init__(self, host, port, user, password='', mode='octet', options={'retries':3, 'tsize':1}):
        super(FBTFTPTransport, self).__init__(host, port, user, password)
        self._mode = mode
        self._options = options
        self._client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._read_size = 0
        if 'timeout' in self._options:
            self._timeout = self._options['timeout']
        else:
            self._timeout = 10
        if 'blksize' in self._options:
            self._blksize = self._options['blksize']
        else:
            self._blksize = DEFAULT_BLKSIZE

    def _sendToServer(self, path):
        if self._mode != 'netascii' and self._mode != 'octet':
            logging.error(
                    "Unexpected mode: %s, expected 'netascii' or 'octet'" % self._mode)
            return ERROR

        packet = []
        packet.append(
                struct.pack(
                    '%dsx%dsx' % (len(path), len(self._mode)), bytes(path.encode('latin-1')), bytes(self._mode.encode('latin-1'))
                    )
                )
        for key, value in self._options.items():
            fmt = str('%dsx%ds' % (len(key), len(str(value))))
            packet.append(
                    struct.pack(
                        fmt, bytes(key.encode('latin-1')), bytes(str(value).encode('latin-1'))
                        )
                    )
        packet.append(b'')
        send_pack = struct.pack('!H', OPCODE_RRQ) +b'\x00'.join(packet)

        self._client_socket.sendto(send_pack, (self._host, self._port))
        return SUCCESS

    def _receiveFromServer(self, path):
        if 'timeout' in self._options or 'blksize' in self._options or 'tsize' in self._options:
            try:
                self._client_socket.settimeout(self._timeout)
                recv_oack, oack_addr = self._client_socket.recvfrom(DEFAULT_BLKSIZE)
                self._client_socket.settimeout(self._timeout)
                oack_head = struct.unpack('!H', recv_oack[:2])[0]
                if oack_head != OPCODE_OACK:
                    error_infos = list(filter(bool, recv_oack[2:].decode('latin-1').split('\x00')))
                    for error_info in error_infos:
                        print(error_info)
                    logging.error(
                            "OACK head error:%s, expected %s" % (oack_head, OPCODE_OACK)
                            )
                    return ERROR
                tokens = list(filter(bool, recv_oack[2:].decode('latin-1').split('\x00')))
                if len(tokens) < 2 or len(tokens) %2 != 0:
                    logging.error(
                            "OACK body error!"
                            )
                    exit(1)
                pos = 0
                while pos < len(tokens):
                    self._options[tokens[pos].lower()] = int(tokens[pos+1])
                    pos += 2

                send_oack = struct.pack('!HH', OPCODE_ACK, 0)
                self._client_socket.sendto(send_oack, oack_addr)
            except socket.timeout:
                logging(
                        "Timeout occured on socket.recvfrom"
                        )
                return ERROR

        file_open = open(path, 'wb')
        threading.Thread(target=self._progressBarShow, args=(path, self._options['tsize'], time.time())).start()
        while True:
            try:
                self._client_socket.settimeout(self._timeout)
                recv_packet, server_addr = self._client_socket.recvfrom((DEFAULT_BLKSIZE+4))
                self._client_socket.settimeout(None)
            except socket.timeout:
                logging.error(
                        "Timeout occurred on socket.recvfrom"
                        )
                file_open.close()
                return ERROR

            recv_head = recv_packet[:4]
            head = struct.unpack('!HH', recv_head)
            if head[0] != OPCODE_DATA:
                error_infos = list(filter(bool, recv_packet[2:].decode('latin-1').split('\x00')))
                for error_info in error_infos:
                    print(error_info)

                logging.error(
                        "Unexpected receive block head: %d expected %d" % (head[0], OPCODE_DATA)
                        )
                return ERROR
            file_open.write(recv_packet[4:])
            self._read_size += len(recv_packet[4:])
            if len(recv_packet[4:]) < self._blksize or self._read_size == self._options['tsize']:
                break
            send_head = struct.pack('!HH', OPCODE_ACK, head[1])
            self._client_socket.sendto(send_head, server_addr)

        return SUCCESS



    def download(self, remote_path, local_path):
        local_path = '/' + local_path.strip('/') + '/' + os.path.basename(remote_path)
        self._sendToServer(remote_path)
        self._receiveFromServer(local_path)
        self._close()

    def upload(self, local_path, remote_path):
        print("fbtftp can not upload file!")

    def _close(self):
        self._client_socket.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
            "-t",
            "--transport",
            type=str,
            default='scp',
            help="Select your desired transfer protocol(scp, ftp, sftp, fbtftp). default scp."
            )
    parser.add_argument(
            "-p",
            "--port",
            default=1969,
            help="port to bind to, fbtftp used."
            )
    parser.add_argument(
            "send_file",
            help="The file you want to transfer"
            )
    parser.add_argument(
            "receive_path",
            help="recerve path"
            )
    
    args = parser.parse_args()
    trans_method = {
            'scp':SCPTransport,
            'sftp':SFTPTransport,
            'ftp':FTPTransport,
            'fbtftp':FBTFTPTransport
            }
    if args.transport not in trans_method:
        print("transport method should be scp sftp ftp fbtftp !")
        exit(0)
    port = {
            'scp': '',
            'sftp':22,
            'ftp':21,
            'fbtftp':args.port
            }

    if len(args.receive_path.split(':')) > 1:
        up_or_down = 'up'
        send_file = args.send_file
        temp = args.receive_path.split(':')
        receive_path = temp[-1]

        user_and_host = temp[0].split('@')
        if len(user_and_host) > 1:
            user = user_and_host[0]
            host = user_and_host[-1]
        else:
            user = getpass.getuser()
            host = user_and_host[0]
    else:
        up_or_down = 'down'
        receive_path = args.receive_path
        if len(args.send_file.split(':')) > 1:
            temp = args.send_file.split(':')
            send_file = temp[-1]
            user_and_host = temp[0].split('@')
            if len(user_and_host) > 1:
                user = user_and_host[0]
                host = user_and_host[-1]
            else:
                user = getpass.getuser()
                host = user_and_host[0]
        else:
            send_file = args.send_file
            user = getpass.getuser()
            host = socket.gethostname()
    if args.transport != 'scp' and args.transport != 'fbtftp':
        password = getpass.getpass(prompt="%s's pass word:" % '@'.join([user, host]))
    else:
        password = ''
    if host == socket.gethostname():
        host = 'localhost'
    if send_file[0] != '/':
        send_file = os.path.abspath(send_file)
    if receive_path[0] != '/':
        receive_path = os.path.abspath(receive_path)
    connect = trans_method.get(args.transport)(host, port.get(args.transport), user, password)
    call_function = {
        'up':connect.upload,
        'down':connect.download
        }
    call_function.get(up_or_down)(send_file, receive_path)




if __name__ == '__main__':
    main()
