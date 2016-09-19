#!/usr/bin/python

import hashlib

def get_md5(file_name, block_size=1024):
    f = open(file_name)
    md5 = hashlib.md5()
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    return md5.hexdigest()


if __name__ == '__main__':
    md5 = get_md5('/home/cn01/soft/wyc.tar')
    print(md5)

