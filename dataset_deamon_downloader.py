import asyncio
import os
from pprint import pprint
import httpx

def df_here():
    # 计算可用空间
    import shutil
    total, used, free = shutil.disk_usage('.')
    print(f'可用: {free // 1024 // 1024 // 1024} GiB')
    return free


async def download_then_delete(client: httpx.AsyncClient, obj, api):
    async with httpx.AsyncClient() as client:
        for _ in range(3):
            resume_byte_pos = 0
            headers = {}
            if os.path.exists(obj['Path']):
                resume_byte_pos = os.path.getsize(obj['Path'])
                headers['Range'] = f'bytes={resume_byte_pos}-'
                print(f'resume from {resume_byte_pos/1024/1024:.2f}MB')
            try:
                async with client.stream('GET', obj['Url'], headers=headers) as r:
                    r.raise_for_status()
                    if resume_byte_pos > 0:
                        assert r.status_code == 206
                    with open(obj['Path'], 'ab') as fp:
                        async for chunk in r.aiter_bytes(chunk_size=16*1024*1024):
                            fp.write(chunk)
                        pass
                print(
                    api.delete_oss_dataset_object(object_name=obj['Path'], dataset_name='aaaaaaaaaaa', namespace='aaaaabbbbccc', revision='master')
                )
                print('deleted remote', obj['Path'])
                return
            except Exception as e:
                print('!!!!!!!!!! retry download', obj['Path'], e)
                pass

def argparser():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--f', type=int, default=1000)
    parser.add_argument('--t', type=int, default=9999)
    return parser.parse_args()

async def main():
    args = argparser()
    from_ :int = args.f
    to_ :int = args.t
    if os.path.exists('stop'):
        print('stop')
        return
    if df_here() < 200 * 1024 * 1024 * 1024: # 200 GiB
        print('low disk space')
        return
    async with httpx.AsyncClient() as client:
        api = ()
        api.login("[__________]")
        objs =  api.??????
        cors = []
        for obj in objs:
            if obj['Type'] == 'dir':
                continue
            if obj['Type'] == 'blob':
                if obj['Path'].endswith('.zip') or obj['Path'].endswith('.keys'):
                    # 下载
                    # ali-draw-20240118-120521.7708.zip
                    random_point = int(obj['Path'].split('.')[1])
                    if not (from_ <= random_point <= to_):
                        print('skip', obj['Path'], 'random_point', random_point, 'not in range', from_, to_)
                        continue
                    pprint(obj)
                    print('downloading', obj['Path'])
                    cors.append(download_then_delete(client, obj, api))

        await asyncio.gather(*cors)


if __name__ == '__main__':
    asyncio.run(main())