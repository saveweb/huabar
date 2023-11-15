import json
import os
import pprint
import time

import httpx
import pymongo
from pymongo.database import Database
from pymongo.collection import Collection
import asyncio

from huabar.utils.util import auto_headers, ran_jid
from huabar.ngx.RegisterDemo1.servlet import Servlet
import requests

END_NOTEID = (19188202 // 1000) * 100
""" 控制 noteid 范围 """ # 别乱改哈

ENABLE_TIMER = False

# func call decorator timer
class AsyncCallTimer:
    def __call__(self, func):
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            r = await func(*args, **kwargs)
            end_time = time.time()
            if ENABLE_TIMER:
                print(f"{func.__name__}() cost {end_time - start_time} seconds")
            return r
        return wrapper

class CallTimer:
    def __call__(self, func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            r = func(*args, **kwargs)
            end_time = time.time()
            if ENABLE_TIMER:
                print(f"{func.__name__}() cost {end_time - start_time} seconds")
            return r
        return wrapper

class Empty(Exception):
    """empty content"""
    pass


class Status:
    TODO = "TODO"
    PROCESSING = "PROCESSING"
    DONE = "DONE"
    EMPTY = "EMPTY"
    """ 无数据，可能是不存在/被删除(?) """

    FAIL = "FAIL"
    FEZZ = "FEZZ"
    """ 特殊: 任务冻结 """


@CallTimer()
def find_max_noteid(collection: Collection):
    r = collection.find_one(
        filter={},
        sort=[("noteid", pymongo.DESCENDING)]
    )
    collection.find_one_and_update
    return r["noteid"] if r else 0

# 验证 noteid 是否连续
def verify_noteid(collection: Collection):
    print("verifying noteid...")
    noteid = 0
    for doc in collection.find(
        filter={},
        sort=[("noteid", pymongo.ASCENDING)]
    ):
        if doc["noteid"] != noteid + 1:
            print(f"noteid: {noteid} -> {doc['noteid']}")
        print(f"verified noteid: {doc['noteid']}", end="\r")
        noteid = doc["noteid"]

def create_notes(collection: Collection, start_noteid: int, end_noteid: int, status: Status = Status.TODO):
    """
    start_noteid: 1, end_noteid: 5
    will create noteid: 1, 2, 3, 4
    """
    assert start_noteid > 0
    assert start_noteid <= end_noteid
    if start_noteid == end_noteid:
        print(f"start_noteid == end_noteid: {start_noteid}")
        return
    docs = []
    for i in range(start_noteid, end_noteid):
        docs.append({
            "noteid": i,
            "status": status,
            "payload": None,
        })
        if len(docs) == 100000:
            s_time = time.time()
            collection.insert_many(docs, ordered=False)
            e_time = time.time()
            docs = []
            print(f"inserted noteid={i} | {e_time - s_time}", end="\r")
    if docs:
        collection.insert_many(docs)
    print(f"inserted noteid={end_noteid}", end="\r")


@CallTimer()
def create_notes_index(collection: Collection):
    collection.create_index(
        keys=[("noteid", pymongo.ASCENDING)],
        unique=True,
        background=True,
    )
    collection.create_index(
        keys=[("status", pymongo.ASCENDING)],
        background=True,
    )


def arg_parser():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mongo", type=str, default="mongodb://localhost:27017")
    return parser.parse_args()


@AsyncCallTimer()
async def process_task(servlet: Servlet, DOC: dict):
    noteid: int = DOC["noteid"]
    print(f"processing noteid: {noteid}")
    r = await servlet.GetNoteInfo(jid=ran_jid(), noteid=noteid)
    
    if r.status_code != 200:
        raise Exception(f"status_code: {r.status_code}")

    if len(r.content) == 0:
        raise Empty("empty content")
    
    try:
        j_data: dict = r.json()
    except Exception:
        print(r.text)
        raise ValueError("json decode error")
    return j_data

    


# 获取一个 task （TODO -> PROCESSING）
@CallTimer()
def claim_task(c_notes: Collection, status: Status = Status.TODO) -> dict:
    doc = c_notes.find_one_and_update(
        filter={"status": status},
        update={"$set": {"status": Status.PROCESSING}},
        sort=[("noteid", pymongo.ASCENDING)],
    )
    return doc

def show_task(c_notes: Collection, noteid: int):
    doc = c_notes.find_one(
        filter={"noteid": noteid},
    )
    pprint.pprint(doc)

@CallTimer()
def update_task(c_notes: Collection, DOC: dict, status: Status, payload: dict):
    to_update = {}
    if DOC["status"] != status:
        to_update["status"] = status
    if DOC["payload"] != payload:
        to_update["payload"] = payload
    
    if not to_update:
        return
    
    c_notes.find_one_and_update(
        filter={"_id": DOC["_id"]},
        update={"$set": to_update},
    )

async def worker(c_notes: Collection, servlet: Servlet):
    while not os.path.exists("stop"):
        # 1. claim a task
        DOC = claim_task(c_notes, status=Status.TODO)
        if not DOC:
            print("no task to claim")
            return

        # reset task
        # print(DOC["noteid"])
        # update_task(c_notes, DOC, Status.TODO, None)
        # continue

        # 2. process task
        try:
            r_json = await process_task(servlet, DOC)
        except Empty as e:
            print(e)
            update_task(c_notes, DOC, Status.EMPTY, None)
            continue
        except Exception as e:
            print(e)
            update_task(c_notes, DOC, Status.FAIL, str(e))
            continue

        # 3. update task
        print(f"DONE noteid: {DOC['noteid']}")
        print(json.dumps(r_json, separators=(',', ':'), ensure_ascii=False)[:300])
        update_task(c_notes, DOC, Status.DONE, r_json)

async def main():
    args = arg_parser()

    h_client = httpx.AsyncClient()
    h_client.headers.update(auto_headers())
    rq_ss = requests.Session()
    rq_ss.headers.update(auto_headers())
    servlet = Servlet(client=h_client, ss=rq_ss)

    m_client = pymongo.MongoClient(args.mongo)

    db: Database= m_client.huabar
    print(db.list_collection_names())

    # !! 删除 notes collection
    # db.drop_collection("notes")

    c_notes: Collection = db.notes
    count_docs_init = c_notes.count_documents(filter={})
    print(f"count_docs_init: {count_docs_init}")
        

    create_notes(c_notes, start_noteid=find_max_noteid(c_notes)+1, end_noteid=END_NOTEID)
    pprint.pprint(c_notes.find_one(filter={"noteid": find_max_noteid(c_notes)}))


    cors = [worker(c_notes=c_notes, servlet=servlet) for _ in range(3)]

    await asyncio.gather(*cors)

    if count_docs_init == 0:
        print("creating index in background...")
        create_notes_index(c_notes)


if __name__ == "__main__":
    asyncio.run(main())