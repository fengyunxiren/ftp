#!/usr/bin/python

from ftplib import FTP
import sys
import os
import time
from multiprocessing import Process
from threading import Thread
import paramiko
import argparse
import getpass
import socket
import hashlib

class FTPClient(object):
    def __init__(self, host, port, user, password):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self.__connect_host()
        self.__total_size = 0
        self.__real_size = 0

    def __connect_host(self):
        self.ftp = FTP()
        self.ftp.connect(self._host, self._port)
        self.ftp.login(self._user, self._password)

    def __del__(self):
        if self.ftp:
            self.ftp.close()

    def download(self, remote_path, local_path, down_name=None, down_start=0, down_size=None, bufsize=1024):
        self.initial_size_record()
        remote_dir = os.path.dirname(remote_path)
        remote_file = os.path.basename(remote_path)
        if down_name == None:
            local_file = local_path.rstrip('/') + '/' + remote_file
        else:
            local_file = local_path.rstrip('/') + '/' + down_name
        self.ftp.cwd(remote_dir)
        self.ftp.voidcmd('TYPE I')
        try:
            file_real_size = self.ftp.size(remote_file)
        except Exception as e:
            print(e)
            return -1
        if down_size == None:
            trans_end = file_real_size
        else:
            trans_end = down_start + down_size if (down_start + down_size) < file_real_size else file_real_size

        self.__total_size = trans_end - down_start

        trans_start = down_start
        if os.path.exists(local_file):
            trans_start += os.stat(local_file).st_size

        if trans_start > trans_end:
            self.__total_size = -1
            print("Warnning! file not match: %s, please delete it and download it again!" % local_file)
        elif trans_start == trans_end:
            self.__total_size = -1
            print("%s is already exist, so it has nonthing to do!" % local_file)

        conn = self.ftp.transfercmd('RETR %s' % remote_file, trans_start)
        file_handler = open(local_file, 'ab')
        trans_cursur = trans_start
        while True:
            data = conn.recv(bufsize)
            if not data:
                break
            elif trans_cursur + len(data) > trans_end:
                data = data[:(trans_end-trans_cursur)]
                file_handler.write(data)
                trans_cursur += len(data)
                self.__real_size = trans_cursur - down_start
                break
            file_handler.write(data)
            trans_cursur += len(data)
            self.__real_size = trans_cursur - down_start
        file_handler.close()
        conn.close()

        return 0


    def progress_bar_download(self, remote_path, local_path, down_name=None, down_start=0, down_size=None, bufsize=1024):
        self.initial_size_record()
        pb = ProgressBar(self)
        t = Thread(target=pb.run, args=())
        t.start()
        try:
            self.download(remote_path, local_path, down_name, down_start, down_size, bufsize)
        except KeyboardInterrupt as e:
            pb.stop = True
            print("\nkeyboard interrupt!")
            return -1
        except Exception as e:
            pb.stop = True
            print(e)
            return -1
        t.join()
        return 0
 
    def upload(self, local_path, remote_path, up_name=None, up_start=0, up_size=None, bufsize=1024):
        self.initial_size_record()
        local_dir = os.path.dirname(local_path)
        local_file = os.path.basename(local_path)
        remote_file = local_file if up_name is None else up_name
        self.ftp.cwd(remote_path)
        self.ftp.voidcmd('TYPE I')
        try:
            file_real_size = os.stat(local_path).st_size
        except Exception as e:
            print(e)
            return -1
        if up_size == None:
            trans_end = file_real_size
        else:
            trans_end = up_start + up_size if (up_start+up_size) < file_real_size else file_real_size

        try:
            trans_start = self.ftp.size(remote_file)
        except:
            trans_start = 0
        trans_start += up_start
        self.__total_size = trans_end - up_start
        if trans_start > trans_end:
            self.__total_size = -1
            print("Warnning! the file you upload is exist, and this two file dose not match!")
            print("if you still want to upload, please change the upload path.")
        elif trans_start == trans_end:
            self.__total_size = -2
            print("file is already exist, so it has nonthing to do!")
        file_handler = open(local_path, 'rb')
        file_handler.seek(trans_start)
        conn, _ = self.ftp.ntransfercmd('STOR %s' % remote_file, trans_start-up_start)
        trans_cursur = trans_start
        self.__real_size = trans_cursur - up_start
        while True:
            data = file_handler.read(bufsize)
            if not data:
                break
            elif trans_cursur + len(data) > trans_end:
                data = data[:(trans_end - trans_cursur)]
                conn.sendall(data)
                trans_cursur += len(data)
                self.__real_size = trans_cursur - up_start
                break
            conn.sendall(data)
            trans_cursur += len(data)
            self.__real_size = trans_cursur - up_start
        conn.close()
        file_handler.close()
        return 0

    def progress_bar_upload(self, local_path, remote_path, up_name=None, up_start=0, up_size=None, bufsize=1024):
        self.initial_size_record()
        pb = ProgressBar(self)
        t = Thread(target=pb.run, args=())
        t.start()
        try:
            self.upload(local_path, remote_path, up_name, up_start, up_size, bufsize)
        except KeyboardInterrupt as e:
            pb.stop = True
            print("\nkeyboard interrupt!")
            return -1
        except Exception as e:
            pb.stop = True
            print(e)
            return -1
        t.join()
        return 0





    def total_size(self):
        return self.__total_size

    def real_size(self):
        return self.__real_size

    def initial_size_record(self):
        self.__real_size = 0
        self.__total_size = 0



class BlockTransport(object):
    def __init__(self, host, port, user, password, trigger_size=512*1024*1024, max_process=5):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self.trigger_size = int(trigger_size)
        self.max_process = int(max_process)
        self.__real_size = 0
        self.__total_size = 0

    def download(self, remote_path, local_path, bufsize=1024):
        self.initial_size_record()

        file_dir = os.path.dirname(remote_path)
        file_name = os.path.basename(remote_path)
        local_file = os.path.join(local_path, file_name)
        trans = FTPClient(self._host, self._port, self._user, self._password)
        trans.ftp.cwd(file_dir)
        trans.ftp.voidcmd('TYPE I')
        try:
            file_size = trans.ftp.size(remote_path)
        except Exception as e:
            self.__total_size = -1
            print(e)
            return -1
        if os.path.exists(local_file) or file_size < self.trigger_size:
            if os.path.exists(local_file):
                if os.stat(local_file).st_size == file_size:
                    self.__total_size = -1
                else:
                    self.__total_size = file_size
            else:
                self.__total_size = file_size
            t = Process(target=trans.download, args=(remote_path, local_path, None, 0, None, bufsize))
            t.start()
            while t.is_alive():
                try:
                    self.__real_size = os.stat(local_file).st_size
                except:
                    self.__real_size = 0
        else:
            down_start = 0
            num_process = 1 + int(file_size/self.trigger_size) if int(file_size/self.trigger_size) < self.max_process else self.max_process
            down_block = file_size / num_process
            threads = []
            part_files = []

            for i in range(1, num_process+1):
                trans = FTPClient(self._host, self._port, self._user, self._password)
                down_name = '.'+file_name + '.part' + str(i) + '-' + str(num_process)
                part_files.append(local_path.rstrip('/')+'/'+down_name)
                t = Process(target=trans.download, args=(remote_path, local_path, down_name, down_start, down_block if i < num_process else None, bufsize))
                threads.append(t)
                down_start += down_block
                if down_start > file_size:
                    break
            self.__total_size = file_size

            for t in threads:
                t.start()
 
            loop = True
            while loop:
                t_status = False
                for t in threads:
                    t_status = t_status or t.is_alive()
                r_size = 0
                for i, part_file in enumerate(part_files):
                    try:
                        tmp = os.stat(part_file).st_size
                    except:
                        tmp = 0
                    if i < len(part_files) - 1 and tmp > down_block:
                        os.system('rm -rf %s' % part_file)
                        exit(1)
                    elif i == len(part_files) -1:
                        if tmp > file_size - down_block * i:
                            os.system('rm -rf %s' % part_file)
                            exit(1)

                    r_size += tmp
                self.__real_size = r_size
                loop = t_status



            for t in threads:
                t.join()




        if self.__real_size == self.__total_size:
            for i, f in enumerate(part_files):
                os.system('cat %s %s %s' % (f, '>>' if i > 0 else '>', local_file))
                os.system('rm -rf %s' % f)
            return 0
        else:
            return -1


    def progress_bar_download(self, remote_path, local_path, bufsize=1024):
        self.initial_size_record()
        pb = ProgressBar(self)
        t = Thread(target=pb.run, args=())
        t.start()
        try:
            state = self.download(remote_path, local_path, bufsize=bufsize)
        except KeyboardInterrupt as e:
            pb.stop = True
            print("\nkeyboard interrupt!")
            print("this is a keyboard interrupt, catch by progress_bar_download")
            return -1
        except Exception as e:
            pb.stop = True
            print(e)
            return -1
        if state != 0:
            pb.stop = True
        t.join()
        return 0

    def upload(self, local_path, remote_path, bufsize=1024):
        self.initial_size_record()
        file_dir = os.path.dirname(local_path)
        file_name = os.path.basename(local_path)
        trans = FTPClient(self._host, self._port, self._user, self._password)
        trans.ftp.cwd(remote_path)
        trans.ftp.voidcmd('TYPE I')
        try:
            file_size = os.stat(local_path).st_size
        except Exception as e:
            self.__total_size = -1
            print(e)
            return
        try:
            remote_size = trans.ftp.size(file_name)
        except:
            remote_size = 0
        if file_size < self.trigger_size or remote_size > 0:
            self.__total_size = file_size
            t = Process(target=trans.upload, args=(local_path, remote_path, None, 0, None, bufsize))
            t.start()
            new_trans = FTPClient(self._host, self._port, self._user, self._password)
            new_trans.ftp.cwd(remote_path)
            new_trans.ftp.voidcmd('TYPE I')
            while t.is_alive():
                try:
                    self.__real_size = new_trans.ftp.size(file_name)
                except:
                    self.__real_size = 0
            if self.__real_size == 0:
                self.__total_size = -1
        else:
            up_start = 0
            num_process = 1 + int(file_size/self.trigger_size) if int(file_size/self.trigger_size) < self.max_process else self.max_process
            up_block = file_size / num_process
            threads = []
            part_files = []
            i = 1
            for i in range(1, num_process+1):
                trans = FTPClient(self._host, self._port, self._user, self._password)
                up_name = '.' + file_name + '.part' + str(i) + '-' + str(num_process)
                part_files.append(os.path.join(remote_path, up_name))
                t = Process(target=trans.upload, args=(local_path, remote_path, up_name, up_start, up_block if i < num_process else None, bufsize))
                threads.append(t)
                up_start += up_block
                if up_start > file_size:
                    break
            self.__total_size = file_size
            for t in threads:
                t.start()

            new_trans = FTPClient(self._host, self._port, self._user, self._password)
            new_trans.ftp.cwd(remote_path)
            new_trans.ftp.voidcmd('TYPE I')
            loop = True
            while loop:
                t_status = False
                for t in threads:
                    t_status = t_status or t.is_alive()
                r_size = 0
                for i, part_file in enumerate(part_files):
                    try:
                        tmp = new_trans.ftp.size(part_file)
                    except:
                        tmp = 0
                    if i < len(part_files)-1 and tmp > up_block:
                        new_trans.ftp.delete(part_file)
                        exit(1)
                    elif i == len(part_file)-1 and tmp > file_size - up_block * i:
                        new_trans.ftp.delete(part_file)
                        exit(1)
                    r_size += tmp
                self.__real_size = r_size
                loop = t_status


            for t in threads:
                t.join()


        if self.__total_size == self.__real_size:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self._host, 22, username=self._user, password=self._password)
            ssh_cm = ''
            for i, part_name in enumerate(part_files):
                ssh_cm += 'cat %s %s %s;rm -rf %s;' % (part_name, '>>' if i > 0 else '>', os.path.join(remote_path, file_name), part_name)
            ssh.exec_command(ssh_cm)
            ssh.close()
            return 0
        else:
            return -1


    def progress_bar_upload(self, local_path, remote_path, bufsize=1024):
        self.initial_size_record()
        pb = ProgressBar(self)
        t = Thread(target=pb.run, args=())
        t.start()
        try:
            state = self.upload(local_path, remote_path, bufsize=bufsize)
        except KeyboardInterrupt as e:
            pb.stop = True
            print("\nkeyboard interrupt!")
            return -1
        except Exception as e:
            pb.stop = True
            print(e)
            return -1
        if state != 0:
            pb.stop = True
        t.join()
        return 0




    def batch_download(self, remote_path, local_path, bufsie=1024):
        remote_dir = os.path.dirname(remote_path)
        remote_file = os.path.basename(remote_path)
        local_file = os.path.join(local_path, remote_file)
        trans = FTPClient(self._host, self._port, self._user, self._password)
        try:
            file_list = trans.ftp.nlst(remote_path)
        except Exception as e:
            print(e)
            return

        if len(file_list) > 1 or file_list[0] != remote_path:
            os.system('mkdir -p %s' % local_file)
            trans.ftp.cwd(remote_path)
            trans_files = trans.ftp.nlst()
            for trans_file in trans_files:
                trans_path = os.path.join(remote_path, trans_file)
                self.batch_download(trans_path, local_file, bufsie)
        else:
            try:
                self.progress_bar_download(remote_path, local_path, bufsie)
            except KeyboardInterrupt as e:
                print("\nkeyboard interrupt")
                exit(0)
            except Exception as e:
                print(e)



    def batch_upload(self, local_path, remote_path, bufsize=1024):
        trans = FTPClient(self._host, self._port, self._user, self._password)
        if os.path.isdir(local_path):
            save_path = os.path.join(remote_path, os.path.basename(local_path))
            try:
                trans.ftp.mkd(save_path)
            except:
                pass
            trans_files = os.listdir(local_path)
            for trans_file in trans_files:
                trans_path = os.path.join(local_path, trans_file)
                self.batch_upload(trans_path, save_path, bufsize)
        else:
            try:
                self.progress_bar_upload(local_path, remote_path, bufsize)
            except KeyboardInterrupt as e:
                print("\nkeyboard interrupt")
                exit(0)
            except Exception as e:
                print(e)


    def total_size(self):
        return self.__total_size

    def real_size(self):
        return self.__real_size

    def initial_size_record(self):
        self.__real_size = 0
        self.__total_size = 0



class ProgressBar(object):
    def __init__(self, call_back, time_flush=0.3):
        self.call_back = call_back
        self.stop = False
        self.time_flush = time_flush
    
    def view_bar(self, count, rate, units='B', rate_units='B/s', real_size=0, total_size=0, used_time=0):
        count_percent = '\r%d%%' % count
        if count < 10:
            count_percent += '  '
        elif count < 100:
            count_percent += ' '
        elif count > 100:
            print("count should less or equal than 100!")
        if units != 'B':
            last_put = '%.1f/%.1f%s' % (real_size, total_size, units)
        else:
            last_put = '%d/%d%s' % (real_size, total_size, units)

        last_put += '  %6.2f %s' % (rate, rate_units)
        last_put += '  %.0f s    ' % used_time
        width = int(os.popen('stty size', 'r').read().split()[-1])
        length_bar = width / 2
        i = length_bar * count / 100
        progress_bar = '[' + i * '>' + (length_bar-i) * ' ' + ']'
        white_length = width - len(count_percent) - len(last_put) - len(progress_bar)
        if white_length < 0:
            white_length += len(progress_bar)
            if white_length < 0:
                white_length += len(last_put)
                if white_length < 0:
                    return
                white_space = white_length * ' '
                sys.stdout.write(count_percent+white_space)
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


    def run(self):
        start_time = time.time()
        units_all = ('B', 'Kb', 'Mb')
        while not self.stop:
            try:
                total_size = self.call_back.total_size()
                real_size = self.call_back.real_size()
            except:
                self.stop = True
                return
            if total_size > 0 and real_size > 0:
                break
            elif total_size < 0:
                return
        rate_time = start_time
        rate_size = real_size
        start_size = real_size
        time.sleep(self.time_flush)
        while not self.stop:
            try:
                real_size = self.call_back.real_size()
            except:
                self.stop = True
                return
            tmp = time.time()
            time_interval = tmp - rate_time
            rate_time = tmp
            count = int(real_size / (total_size / 100)) if real_size > 1024 else int(real_size / total_size * 100)
            rate = (real_size - rate_size) / time_interval
            rate_size = real_size
            time_left = float('inf') if rate-0 < 0.000001 else (total_size-real_size)/rate
            t_real_size = real_size
            t_total_size = total_size
            for index in range(len(units_all)):
                if t_total_size > 1024 * 10 and index != len(units_all) - 1:
                    t_total_size /= 1024
                    t_real_size /= 1024
                else:
                    break
            units = units_all[index]
            if real_size == total_size:
                count = 100
                time_interval = tmp - start_time
                rate = (real_size - start_size) / time_interval
                time_left = time_interval
                self.stop = True

            for index in range(len(units_all)):
                if rate > 1024 and index != len(units_all) - 1:
                    rate /= 1024
                else:
                    break
            rate_units = units_all[index] + '/s'
            self.view_bar(count, rate, units, rate_units, t_real_size, t_total_size, time_left)
            time.sleep(self.time_flush)
            if count == 100:
                print('\n')


def get_md5(file_name, bufsize=2048):
    f = open(file_name)
    md5 = hashlib.md5()
    while True:
        data = f.read(bufsize)
        if not data:
            break
        md5.update(data)
    return md5.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
            "-p",
            "--port",
            default=21,
            help="port for ftp server"
            )
    parser.add_argument(
            "-m",
            "--max_process",
            default=5,
            help="max number of process to use"
            )
    parser.add_argument(
            "-t",
            "--trigger_size",
            default=128,
            help="when file size large than trigger size, using multi process. units(Mb)."
            )
    parser.add_argument(
            "-b",
            "--bufsize",
            default=1024,
            help="the size in per transfer. units(Byte)"
            )
    parser.add_argument(
            "send_file",
            help="The file you want to transfer"
            )
    parser.add_argument(
            "receive_path",
            help="receive path"
            )
    args = parser.parse_args()

    if len(args.receive_path.split(':')) == 2 and len(args.send_file.split(':')) == 1:
        up_or_down = 'up'
        send_file = args.send_file
        receive_path = args.receive_path.split(':')[1]

        user_and_host = args.receive_path.split(':')[0].split('@')
        if len(user_and_host) == 1:
            user = getpass.getuser()
            host = user_and_host[0]
        elif len(user_and_host) == 2:
            user = user_and_host[0]
            host = user_and_host[1]
        else:
            print("download file: %s [-p port][-m max_process][-t trigger_size][-b bufsize] user@host:/file/path /receive/path" % sys.argv[0])
            print("upload file: %s [-p port][-m max_process][-t trigger_size][-b bufsize] /file/path user@host:/receive/path" % sys.argv[0])
            return
    elif len((args.send_file).split(':')) == 2 and len(args.receive_path.split(':')) == 1:
        up_or_down = 'down'
        receive_path = args.receive_path
        send_file = args.send_file.split(':')[1]
        user_and_host = args.send_file.split(':')[0].split('@')
        if len(user_and_host) == 1:
            user = getpass.getuser()
            host = user_and_host[0]
        elif len(user_and_host) == 2:
            user = user_and_host[0]
            host = user_and_host[1]
        else:
            print("download file: %s [-p port][-m max_process][-t trigger_size][-b bufsize] user@host:/file/path /receive/path" % sys.argv[0])
            print("upload file: %s [-p port][-m max_process][-t trigger_size][-b bufsize] /file/path user@host:/receive/path" % sys.argv[0])
            return
    elif len(args.receive_path.split(':')) == 1 and len(args.send_file.split(':')) == 1:
        up_or_down = 'down'
        receive_path = args.receive_path
        send_file = args.send_file
        host = 'localhost'
        user = getpass.getuser()
    else:
        print("download file: %s [-p port][-m max_process][-t trigger_size][-b bufsize] user@host:/file/path /receive/path" % sys.argv[0])
        print("upload file: %s [-p port][-m max_process][-t trigger_size][-b bufsize] /file/path user@host:/receive/path" % sys.argv[0])
        return
    if host == socket.gethostname():
        host = 'localhost'
    if send_file[0] != '/':
        send_file = os.path.abspath(send_file)
    if receive_path[0] != '/':
        receive_path = os.path.abspath(receive_path)
    port = args.port
    password = getpass.getpass(prompt="%s's pass word:" % '@'.join([user, host]))
    trigger_size = int(args.trigger_size) * 1024 * 1024
    max_process = int(args.max_process)
    bufsize = int(args.bufsize)
    connect = BlockTransport(host, port, user, password, trigger_size, max_process)
    call_function = {
            'up': connect.batch_upload,
            'down': connect.batch_download
            }
    try:
        state = call_function.get(up_or_down)(send_file, receive_path, bufsize)
    except Exception as e:
        print(e)
        return
    if state != 0:
        return



    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, 22, username=user, password=password)

    if up_or_down == 'down':
        stdin, stdout, stderr = ssh.exec_command('md5sum %s' % send_file)
        remote_md5 = stdout.read().split(' ')[0]
        local_md5 = get_md5(os.path.join(receive_path, os.path.basename(send_file)))
        if local_md5 != remote_md5:
            print("The file you download is broken, please download it again!")
            return
        else:
            print("download successed!")
    elif up_or_down == 'up':
        local_md5 = get_md5(send_file)
        stdin, stdout, stderr = ssh.exec_command(' md5sum %s' % os.path.join(receive_path, os.path.basename(send_file)))
        remote_md5 = stdout.read().split(' ')[0]
        if local_md5 != remote_md5:
            print("The file you upload is broken, please upload it again!")
            return
        else:
            print("upload successed!")












if __name__ == '__main__':
    main()
    #connect = BlockTransport('127.0.0.1', 21, 'cn01', 'airation', 128*1024*1024, 5)
    #connect.batch_download('/home/cn01/soft/wyc.tar', '/home/cn01/Transtest/', 1024)
    #connect = FTPClient('127.0.0.1', 21, 'cn01', 'airation')
    #connect.progress_bar_download('/home/cn01/soft/wyc.tar', '/home/cn01/Transtest/', 16)
