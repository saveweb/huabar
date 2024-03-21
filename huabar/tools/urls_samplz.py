import httpx
import asyncio
import random
from asyncio import Queue

async def main():
    # with open('notecontent.oss-cn-hangzhou.aliyuncs.com.urls.list', 'r') as f:
    with open('urllist.txt', 'r') as f:
        # urls = [url.strip() for url in f.readlines() if random.random() < 0.001]
        urls = []
        for line in f:
            # if random.random() < 0.001:
            urls.append(line.strip())
    len_urls = len(urls)
    print(len_urls, 'urls loaded')
    async with httpx.AsyncClient() as client:
        sample_size = 3000

        sample_total_content_length = 0
        jobs = random.sample(urls, sample_size)
        del urls
        print('jobs:', len(jobs))
        queue = Queue()
        for job in jobs:
            queue.put_nowait(job)
        del jobs

        async def worker():
            nonlocal sample_total_content_length
            while not queue.empty():
                url = await queue.get()
                size = await get_content_length(client, url)
                sample_total_content_length += size
                print(url, size // 1024, 'kb', sample_total_content_length)
                queue.task_done()

        await asyncio.gather(*[worker() for _ in range(20)])

        print('len_urls:', len_urls, '取样量:', sample_size, '样本得到的 content_length 大小:', sample_total_content_length, '样本占比:', sample_size / len_urls)
        print('样本平均 content_length:',  sample_total_content_length // sample_size)
        print('全部 urls 大概有多大:', f'({len_urls * (sample_total_content_length / sample_size) / 1024 / 1024 // 1024} GiB)')


async def get_content_length(client: httpx.AsyncClient, url: str):
    for _ in range(3):
        try:
            r = await client.head(url)
            return int(r.headers['Content-Length'])
        except Exception as e:
            pass

    print('!!!!!!!!!! Failed to get content length of', url)
    return 0

if __name__ == '__main__':
    asyncio.run(main())