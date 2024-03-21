import httpx
import asyncio
import os
import tarfile
import tempfile

from huabar.utils.urls import get_domain, hangzhou_internal_url


# 并发获取文件，写入 tar 中
async def main():
    stop = False
    urls_file = 'notecontent.oss-cn-hangzhou.aliyuncs.com.urls.list'
    tarball_size = 5 * 1024 * 1024  # 2GiB
    urls_queue = asyncio.Queue(maxsize=10000)

    tarfile_path = 'tarfile_tmp.tar'
    if os.path.exists(tarfile_path):
        os.remove(tarfile_path)
    tarfile_f = tarfile.open(tarfile_path, 'w', format=tarfile.PAX_FORMAT)
    print('tarfile opened')
    with open(urls_file, 'r') as f:
        for url in f:
            url = url.strip()
            if not url:
                continue
            print('put', url)
            await urls_queue.put(url)
    async def worker(client: httpx.AsyncClient):
        nonlocal stop
        nonlocal tarfile_f
        while not stop:
            url = await urls_queue.get()
            print(url)
            # stream
            tmpf = tempfile.TemporaryFile()
            async with client.stream('GET', url) as r:
            # async with client.stream('GET', hangzhou_internal_url(url)) as r:
                async for chunk in r.aiter_bytes():
                    tmpf.write(chunk)
            # 添加 pax 记录
            domain = get_domain(url)
            key = url.split(domain + '/', 1)[1]
            # pax_headers = {"": 123}
            tarinfo = tarfile.TarInfo(name=f'{domain}/{key}')
            tarfile_f.pax_headers = {"": "123"}
            tarfile_f.addfile(tarinfo, tmpf)

            if tarfile_f.fileobj.tell() > tarball_size:
                stop = True

            urls_queue.task_done()
    client = httpx.AsyncClient()
    workers = [await asyncio.create_task(worker(client=client)) for _ in range(20)]
    
    await asyncio.gather(*workers)
    
    await urls_queue.join()
    print('done')
    await client.aclose()




if __name__ == '__main__':
    asyncio.run(main())