import os
import copy
import time
import zipfile
import datetime
import shutil
import httpx
import asyncio
import random
import traceback

RETRY = 3
PACKAGE_SIZE = 5 * 1024 * 1024 * 1024 # 1 GiB
DISABLE_ALI = True

async def _upload_chunk(client: httpx.AsyncClient, server_url, filename, chunk_id, chunk):
    files = {'file': (filename, chunk)}
    err = None
    for i in range(5):
        try:
            response = await client.post(f'{server_url}/upload/{filename}/{chunk_id}', files=files)
            print(response.text)
            response.raise_for_status()
            return True
        except Exception as e:
            err = e
            wait = random.randint(15, 30) * (i + 1)
            print('retry upload chunk', chunk_id, 'after', wait, 's')
            await asyncio.sleep(wait)

    traceback.print_exc()
    raise Exception(f'upload chunk failed ({chunk_id}): {err})')

async def upload_file(source_file_path, server_url = 'http://[__________]', ths = 10, check_free = True):
    chunk_size = 5 * 1024 * 1024  # MB
    lastchunk_size = -1
    filename = os.path.basename(source_file_path)

    tasks: list[asyncio.Task] = []

    def tasks_contrl():
        nonlocal tasks
        tasks = [task for task in tasks if not task.done()]

    async with httpx.AsyncClient(timeout=60) as client:
        while check_free and (await client.get(f'{server_url}/free')).json()['GiB'] < 50: #
            print('waiting for free space')
            await asyncio.sleep(60)
        with open(source_file_path, 'rb') as f:
            chunk_id = 0
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                lastchunk_size = len(chunk)
                task = _upload_chunk(client, server_url, filename, chunk_id, chunk)
                tasks.append(asyncio.create_task(task))
                if len(tasks) >= ths:
                    await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    tasks_contrl()

                chunk_id += 1


        while len(tasks) > 0:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            tasks_contrl()

        maxid = chunk_id - 1
        r = await client.get(f'{server_url}/complete/{filename}/{maxid}/{chunk_size}/{lastchunk_size}')
        print(r.text)
        r.raise_for_status()
        if r.status_code != 200:
            raise Exception(f'upload failed: {r.text}')
        assert r.json()['status'] == 'ok'
        return True





async def get_qiniu_content(client: httpx.AsyncClient, key: str):
    r = await client.get(f'http://free-qvm-nbxs-gate-io.qiniu.com/{key}',
                         headers={"Host":"haowanlab.qiniudn.com"})
    r.raise_for_status()
    return r.content

async def get_ali_content(client: httpx.AsyncClient, key: str):
    r = await client.get(f'http://haowanlab.oss-cn-hangzhou-internal.aliyuncs.com/{key}')
    r.raise_for_status()
    return r.content

def du_dir(path: str):
    size = 0
    for root, dirs, files in os.walk(path):
        size += sum([os.path.getsize(os.path.join(root, name)) for name in files])
    return size

def ls_dir(path: str):
    # huabar/ai/tools.py
    # huabar/ai/urls_csv_ana.py
    # ....
    for root, dirs, files in os.walk(path):
        for name in files:
            yield os.path.join(root, name)

async def main():
    os.makedirs('qiniu', exist_ok=True)
    os.makedirs('ali', exist_ok=True)

    du_qiniu_size = du_dir('qiniu')
    du_ali_size = du_dir('ali')

    qiniu_todo_keys = asyncio.Queue()
    ali_todo_keys = asyncio.Queue()

    qiniu_downloaded_keys = set()
    qiniu_downloaded_keys_new_in_this_turn = set() # 用于判断是否有新的 key 下载
    ali_downloaded_keys = set()


    os.system('cat progress_draw/haowanlab.qiniu.downloaded.keys.* > progress_draw/haowanlab.qiniu.downloaded.keys')
    with open('progress_draw/haowanlab.qiniu.downloaded.keys') as f:
        for line in f:
            qiniu_downloaded_keys.add(line.strip()) if line.strip() else None

    def flush_qiniu_downloaded_new_keys():
        nonlocal qiniu_downloaded_keys_new_in_this_turn
        with open('progress_draw/haowanlab.qiniu.downloaded.keys', 'a') as f:
            print("putting", len(qiniu_downloaded_keys_new_in_this_turn), "keys")
            for key in qiniu_downloaded_keys_new_in_this_turn:
                f.write(key + '\n')
        os.system('split -b 20M progress_draw/haowanlab.qiniu.downloaded.keys progress_draw/haowanlab.qiniu.downloaded.keys.')

    with open('progress_draw/haowanlab.ali.downloaded.keys.bin') as f:
        for line in f:
            ali_downloaded_keys.add(line.strip()) if line.strip() else None
    def flush_ali_downloaded_keys():
        nonlocal ali_downloaded_keys
        with open('progress_draw/haowanlab.ali.downloaded.keys.bin', 'w') as f:
            for key in ali_downloaded_keys:
                f.write(key + '\n')

    async def qiniu_download_worker(client: httpx.AsyncClient, queue: asyncio.Queue):
        nonlocal du_qiniu_size
        nonlocal qiniu_downloaded_keys
        nonlocal qiniu_downloaded_keys_new_in_this_turn
        while not queue.empty() and du_qiniu_size < PACKAGE_SIZE:
            key = await queue.get()
            for i in range(RETRY):
                try:
                    content = await get_qiniu_content(client, key)
                    os.makedirs(os.path.dirname(f'qiniu/{key}'), exist_ok=True)
                    with open(f'qiniu/{key}', 'wb') as f:
                        f.write(content)
                    qiniu_downloaded_keys.add(key)
                    qiniu_downloaded_keys_new_in_this_turn.add(key)
                    du_qiniu_size += len(content)
                    break
                except Exception as e:
                    if i == RETRY - 1:
                        print('!!!!!!!!!! Failed to get content of', key, e)
            queue.task_done()

    async def ali_download_worker(client: httpx.AsyncClient, queue: asyncio.Queue, downloaded_keys: set):
        nonlocal du_ali_size
        while not queue.empty() and du_ali_size < PACKAGE_SIZE:
            print('du_ali_size:', du_ali_size//1024//1024//1024, 'GiB') if random.random() < 0.01 else None
            key = await queue.get()
            for i in range(RETRY):
                try:
                    content = await get_ali_content(client, key)
                    os.makedirs(os.path.dirname(f'ali/{key}'), exist_ok=True)
                    with open(f'ali/{key}', 'wb') as f:
                        f.write(content)
                    downloaded_keys.add(key)
                    du_ali_size += len(content)
                    break
                except Exception as e:
                    if i == RETRY - 1:
                        print('!!!!!!!!!! Failed to get content of', key, e)
            queue.task_done()

    async with httpx.AsyncClient() as client:
        with open('progress_draw/haowanlab.qiniu.keys.bin') as f:
            for line in f:
                qiniu_todo_keys.put_nowait(line.strip()) if line.strip() not in qiniu_downloaded_keys else None
        print('qiniu_todo_keys:', qiniu_todo_keys.qsize())
        with open('progress_draw/haowanlab.ali.keys.bin') as f:
            for line in f:
                ali_todo_keys.put_nowait(line.strip()) if line.strip() not in ali_downloaded_keys else None
        print('ali_todo_keys:', ali_todo_keys.qsize())

        while not qiniu_todo_keys.empty() and not os.path.exists('stop'):
            for file in os.listdir():
                if file.endswith('.zip') or file.endswith('.keys'):
                    # print('uploading', file)
                    os.remove(file)
            if not qiniu_todo_keys.empty():
                print('downloading qiniu...', datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
                await asyncio.gather(*[qiniu_download_worker(client, qiniu_todo_keys) for _ in range(100)])
                print('downloaded qiniu', datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
                print('du_qiniu_size:', du_qiniu_size//1024//1024//1024, 'GiB')
                assert du_qiniu_size == du_dir('qiniu')
                keys_in_this_packages = []
                for path in ls_dir('qiniu'):
                    # path: ali/123
                    # key = path[4:]
                    # path: qiniu/cadsad
                    key = path[6:]
                    keys_in_this_packages.append(key)
                # 存到 zip 里
                print('keys_in_this_packages:', len(keys_in_this_packages))
                zip_filename = f'qiniu-draw-{datetime.datetime.now().strftime("%Y%m%d-%H%M%S")}.{random.randint(1000, 9999)}.zip'
                with zipfile.ZipFile(zip_filename, 'w') as zipf:
                    for key in keys_in_this_packages:
                        zipf.write(f'qiniu/{key}', f'qiniu/{key}')
                print('zip done', datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
                zip_metadata = zip_filename + '.keys'
                with open(zip_metadata, 'w') as f:
                    for key in keys_in_this_packages:
                        assert key in qiniu_downloaded_keys
                        assert key in qiniu_downloaded_keys_new_in_this_turn
                        f.write(key + '\n')
                print('zip_metadata done')
                os.makedirs('qiniu', exist_ok=True)

                for i in range(RETRY):
                    try:
                        await upload_file(zip_filename)
                        break
                    except Exception:
                        print('!!!!!!!!!! retry upload', zip_filename)
                        if i == RETRY - 1:
                            raise Exception('upload failed')
                os.remove(zip_filename)
                for i in range(RETRY):
                    try:
                        await upload_file(zip_metadata, check_free = False)
                        break
                    except Exception:
                        print('!!!!!!!!!! retry upload', zip_metadata)
                        if i == RETRY - 1:
                            raise Exception('upload failed')
                os.remove(zip_metadata)
                flush_qiniu_downloaded_new_keys()
                qiniu_downloaded_keys_new_in_this_turn = set() # clear
                print('flush_qiniu_downloaded_keys done')
                shutil.rmtree('qiniu')
                print('rmtree done')
                du_qiniu_size = 0

                os.remove('progress_draw/haowanlab.qiniu.downloaded.keys') # 先删

                os.system("cd progress_draw && git add . ")
                os.system("cd progress_draw && git commit -a -m " + zip_filename)
                os.system("cd progress_draw && git push")

                os.system('cat progress_draw/haowanlab.qiniu.downloaded.keys.* > progress_draw/haowanlab.qiniu.downloaded.keys') # 再恢复

        time.sleep(5)

if __name__ == '__main__':
    # await main()
    asyncio.run(main())
    pass