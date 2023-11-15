
import asyncio
import pprint
import httpx
from ngx.RegisterDemo1.servlet import Servlet
from utils.util import auto_headers, ran_jid

async def task(servlet: Servlet, noteid: int):
    r = await servlet.GetNoteInfo(ran_jid(), noteid)
    if len(r.content) == 0:
        print(noteid, "empty content")
        return
    r_json = r.json()
    pprint.pprint(r_json)
    if r_json.get("elementid"):
        print("3D", r_json["elementid"])
    noteossurl = r_json.get('noteossurl')
    original_url = r_json['original_url']
    if noteossurl:
        noteossurl_size = int((await servlet.client.head(noteossurl)).headers['Content-Length'])
    else:
        noteossurl_size = 0
    if "imax.vmall.com" not in original_url and "deletepic.png" not in original_url:
        original_url_size = int((await servlet.client.head(original_url)).headers['Content-Length'])
    else:
        original_url_size = 0
    print(noteid, ", oss", noteossurl_size//1024, "KB", noteossurl, "img", original_url_size//1024, "KB", original_url)

    return (noteossurl_size, original_url_size)

async def main():

    # 平均从 200000 到 19188202 选出 100 个 noteid

    noteids=[
        nid for nid in range(200000, 19188202, (19188202-200000)//100)
    ]
    noteids.pop()

    h_client = httpx.AsyncClient()
    h_client.headers.update(auto_headers())
    servlet = Servlet(client=h_client)

    tasks = [task(servlet, noteid) for noteid in noteids]

    results = await asyncio.gather(*tasks)

    print("总计:")
    print("oss", sum([r[0] for r in results])/1024/1024, "MiB")
    print("img", sum([r[1] for r in results])/1024/1024, "MiB")
    
if __name__ == "__main__":
    asyncio.run(main())