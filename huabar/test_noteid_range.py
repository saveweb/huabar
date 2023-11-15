
import asyncio
import httpx
from ngx.RegisterDemo1.servlet import Servlet
from utils.util import auto_headers, ran_jid

async def get_size(sam: asyncio.Semaphore,servlet: Servlet, noteid: int):
    async with sam:
        r = await servlet.GetNoteInfo(ran_jid(), noteid)
        if len(r.content) == 0:
            print(noteid, "empty content")
            return
        r_json = r.json()
        if r_json.get("elementid"):
            print("3D", r_json["elementid"])
        noteossurl = r_json.get('noteossurl')
        try:
            if noteossurl:
                noteossurl_size = int((await servlet.client.head(noteossurl)).headers['Content-Length'])
            else:
                noteossurl_size = 0

            original_url = r_json['original_url']
            if "imax.vmall.com" not in original_url and "deletepic.png" not in original_url and original_url:
                original_url_size = int((await servlet.client.head(original_url)).headers['Content-Length'])
            else:
                original_url_size = 0
            if "imax.vmall.com" in original_url or "deletepic.png" in original_url:
                original_url += " (失效)"
        except Exception as e:
            print(noteid, e)
            return
        print(noteid,r_json.get("jid"), "过程数据", noteossurl_size//1024, "KB", noteossurl, "原图", original_url_size//1024, "KB", original_url)

        return (noteossurl_size, original_url_size)

async def main():

    # 平均从 START 到 END 选出约 X 个 noteid 测试
    X = 500
    START = 1
    END = 19188202

    noteids=[
        nid for nid in range(START, END, (END-START)//X)
    ]
    print(len(noteids))

    transport = httpx.AsyncHTTPTransport(
        retries=2,
    )
    h_client = httpx.AsyncClient(transport=transport)
    h_client.headers.update(auto_headers())
    servlet = Servlet(client=h_client)

    sam = asyncio.Semaphore(7)

    tasks = [get_size(sam, servlet, noteid) for noteid in noteids]

    # results = await asyncio.gather(*tasks)
    results = []
    pack = []
    done = 0
    for task in tasks:
        pack.append(task)
        done += 1
        if done%10 == 0:
            results += await asyncio.gather(*pack)
            pack = []
            print("%.2f%%" % (len(results)/len(tasks)*100))
    if pack:
        results += await asyncio.gather(*pack)

    print("总计:")
    print("oss", sum([r[0] for r in results if r is not None])/1024/1024, "MiB")
    print("img", sum([r[1] for r in results if r is not None])/1024/1024, "MiB")
    
if __name__ == "__main__":
    asyncio.run(main())