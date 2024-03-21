import os
import importlib.util
from pathlib import Path

from internetarchive import get_item

############## LOCK ##############

class AlreadyRunningError(Exception):
    def __init__(self, message: str=""):
        self.message = message
        super().__init__(self.message)
    def __str__(self):
        return self.message


LOCK_FILENAME = '_uploading.lock'

class UploadLock_Basic:
    def __init__(self, lock_dir, lock_name):
        self.lock_file = os.path.join(lock_dir, lock_name+LOCK_FILENAME)

    def __enter__(self):
        if os.path.exists(self.lock_file):
            with open(self.lock_file, 'r', encoding='utf-8') as f:
                print(f.read())
            print("Another instance is already running.")
            raise AlreadyRunningError('Another instance is already running.')
        else:
            with open(self.lock_file, 'w', encoding='utf-8') as f:
                f.write(f'PID: {os.getpid()}: Running')
            # print("Acquired lock, continuing.")

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.remove(self.lock_file)
        # print("Released lock.")

    # decorator
    def __call__(self, func):
        def wrapper(*args, **kwargs):
            with self:
                return func(*args, **kwargs)
        return wrapper


class UploadLock_Fcntl():
    fcntl = None
    try:
        import fcntl
    except ModuleNotFoundError:
        pass

    def __init__(self, lock_dir, lock_name):
        if self.fcntl is None:
            raise(ModuleNotFoundError("No module named 'fcntl'", name='fcntl'))
        
        self.lock_file = os.path.join(lock_dir, lock_name+LOCK_FILENAME)
        self.lock_file_fd = None

    def __enter__(self):
        assert self.fcntl is not None
        self.lock_file_fd = open(self.lock_file, 'w')
        try:
            self.fcntl.lockf(self.lock_file_fd, self.fcntl.LOCK_EX | self.fcntl.LOCK_NB)
            # print("Acquired lock, continuing.")
        except IOError:
            raise AlreadyRunningError("Another instance is already running.")
            

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert self.fcntl is not None
        if self.lock_file_fd is None:
            raise IOError("Lock file not opened.")
        self.fcntl.lockf(self.lock_file_fd, self.fcntl.LOCK_UN)
        self.lock_file_fd.close() # lock_file_fd.close() 之后，其他进程有机会在本进程删掉锁文件之前拿到新锁
        try:
            os.remove(self.lock_file) # 删除文件不影响其他进程已持有的 inode 新锁
        except FileNotFoundError:
            # 如果抢到新锁的是本进程，删除文件的是其他进程，那么本进程再删除时自然会 FileNotFoundError，忽略就好
            pass
        # print("Released lock.")

    # decorator
    def __call__(self, func):
        def wrapper(*args, **kwargs):
            with self:
                return func(*args, **kwargs)
        return wrapper

class UploadLock():
    def __new__(cls, lock_dir, lock_name=""):
        fcntl_avaivable = importlib.util.find_spec('fcntl')
        if fcntl_avaivable is not None:
            return UploadLock_Fcntl(lock_dir, lock_name)
        else:
            return UploadLock_Basic(lock_dir, lock_name)
        
############## END OF LOCK ##############



def read_ia_keys(keysfile) -> tuple[str, str]:
    ''' Return: tuple(`access_key`, `secret_key`) '''
    with open(keysfile, 'r', encoding='utf-8') as f:
        key_lines = f.readlines()

    access_key = key_lines[0].strip()
    secret_key = key_lines[1].strip()

    return access_key, secret_key
access_key, secret_key= read_ia_keys(os.path.expanduser('~/huabar_ia_keys.txt'))


def uplaod(identifier: str, metadata: dict,
           zip_ia_path: str, zippath: Path,
           keys_ia_path: str, keyspath: Path):
    # haowanlab.oss-cn-hangzhou.aliyuncs.com
    # haowanlab.qiniudn.com
    filedict = { # "remote filename": "local filename"
        zip_ia_path: zippath,
        keys_ia_path: keyspath,
        'huabar_logo_itemimage.png': os.path.expanduser('~/huabar_logo_itemimage.png'),
    }
    item = get_item(identifier)
    for file_in_item in item.files:
        if file_in_item["name"] in filedict:
            filedict.pop(file_in_item["name"])
            print(f"File {file_in_item['name']} already exists in {identifier}.")

    print(filedict)

    r = item.upload(
        files=filedict,
        metadata=metadata,
        access_key=access_key,
        secret_key=secret_key,
        verbose=True,
    )

    return True

def main():
    for file in os.listdir():
        if os.path.exists('stop'):
            print('Stop')
            return

        if file.endswith('.zip'):
            zipfile = file
            if os.path.exists(zipfile+'.up2ia_mark'):
                print('Already uploaded', zipfile, end='   \r')
                continue


            zippath = Path(zipfile)
            keyspath = Path(zipfile + '.keys')
            if not os.path.exists(keyspath):
                print('No keys file for', zippath)
                continue

            metadata = {
                "mediatype": "web",
                "collection": "opensource_media",
                "description":
"""<a href="https://web.archive.org/web/20230401003921/http://www.haowanlab.com/">画吧</a>的绘画(`note`)工程文件(`noteossurl`)及图片(`original_url`)。<br>
huabar's drawing (note) project files (`noteossurl`) and images (`original_url`).<br>
<br>
查看 STWP Wiki 了解如何使用这些数据：<br>
Check out STWP Wiki to learn how to use this data:<br>
<br>
<li><a href="https://wiki.saveweb.org/画吧">https://wiki.saveweb.org/画吧</a> (ZH)</li>
<li><a href="https://wiki.saveweb.org/en:画吧">https://wiki.saveweb.org/en:画吧</a> (EN)</li>""",
                "subject": ['画吧','huabar','haowanlab.com', 'STWP'],
            }

            if 'qiniu-draw' in zipfile:
                identifier = 'huabar_' + zipfile[:len('qiniu-draw-20240129-22')]
                metadata['title'] = '画吧 - ' + zipfile[:len('qiniu-draw-20240129-22')]
                metadata['source'] = 'http://haowanlab.qiniudn.com'
            elif 'ali-draw' in zipfile:
                identifier = 'huabar_' + zipfile[:len('ali-draw-20240129-22')]
                metadata['title'] = '画吧 - ' + zipfile[:len('ali-draw-20240129-22')]
                metadata['source'] = 'http://haowanlab.oss-cn-hangzhou.aliyuncs.com'
            else:
                raise ValueError('Unknown file: ' + zipfile)


            
            try:
                with UploadLock('./', zipfile):
                    print(identifier)
                    print(metadata)
                    result = uplaod(identifier, metadata,
                        zippath.name, zippath,
                        keyspath.name, keyspath)
                    if result is True:
                        print('Uploaded', zipfile, 'to', identifier)
                        with open(zipfile+'.up2ia_mark', 'w', encoding='utf-8') as f:
                            f.write('Uploaded to IA:' + identifier)
                    else:
                        raise ValueError('Upload failed')
            except AlreadyRunningError:
                print('Another instance is uploading', zipfile)
                continue

if __name__ == '__main__':
    main()