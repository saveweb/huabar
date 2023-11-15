from dataclasses import dataclass
import datetime
import json
import os
import pprint
import argparse
import time
from typing import Optional

import httpx
import pymongo
from bson import ObjectId, Timestamp
from pymongo.database import Database
from pymongo.collection import Collection
import asyncio

from huabar.utils.util import AsyncCallTimer, CallTimer, auto_headers, ran_jid
from huabar.ngx.RegisterDemo1.servlet import Servlet
import requests

END_NOTEID = (19188202 // 1000) * 1
""" 控制队列 noteid 范围 """ # 别乱改哈

class Status:
    TODO = "TODO"
    PROCESSING = "PROCESSING"
    DONE = "DONE"
    EMPTY = "EMPTY"
    """ 无数据，可能是不存在/被删除(?) """

    FAIL = "FAIL"
    FEZZ = "FEZZ"
    """ 特殊: 任务冻结 """

@dataclass
class Task:
    _id: ObjectId
    noteid: int
    status: Status

    claim_at: Optional[Timestamp] = None
    update_at: Optional[Timestamp] = None


    def __post_init__(self):
        assert self.status in Status.__dict__.values()

    def __repr__(self):
        return f"Task(noteid={self.noteid}, status={self.status})"
    
@dataclass
class Note:
    _id: ObjectId
    noteid: int
    status: Status
    payload: dict

class Empty(Exception):
    """empty content"""
    pass



@CallTimer()
def find_max_noteid(collection: Collection):
    r = collection.find_one(
        filter={},
        sort=[("noteid", pymongo.DESCENDING)]
    )
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

def init_queue(notes_queue: Collection, start_noteid: int, end_noteid: int, status: str = Status.TODO):
    """
    start_noteid: 1, end_noteid: 5
    will create noteid: 1, 2, 3, 4

    doc: {"noteid": int,"status": str}
    """
    assert notes_queue.name == "notes_queue"
    assert status in Status.__dict__.values()
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
        })
        if len(docs) == 100000:
            s_time = time.time()
            notes_queue.insert_many(docs, ordered=False)
            e_time = time.time()
            docs = []
            print(f"inserted c_queue={i} | {e_time - s_time}", end="\r")
    if docs:
        notes_queue.insert_many(docs)
    print(f"inserted c_queue={end_noteid}", end="\r")


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
    @dataclass
    class Args:
        mongo: str = "mongodb://localhost:27017"
        """ mongodb://xxx:yy@zzz:1111 """
        task_provider: bool = False
        """ 定义为任务提供者，全局只能有一个 """
        end_noteid: int = END_NOTEID
        """ 任务队列结束的*大概 noteid (任务提供者) 精度为 +- qos """
        qos: int = 8
        """ 每秒生成任务数 (任务提供者) """

    parser = argparse.ArgumentParser()
    parser.add_argument("--mongo",          type=str,   default=Args.mongo,         help=Args.mongo)
    parser.add_argument("--task_provider",  action="store_true",  default=False,    help=str(Args.task_provider))
    parser.add_argument("--end_noteid",     type=int,   default=Args.end_noteid,    help=str(Args.end_noteid))
    parser.add_argument("--qos",            type=int,   default=Args.qos,           help=str(Args.qos))

    return Args(**vars(parser.parse_args()))


@AsyncCallTimer()
async def process_task(servlet: Servlet, TASK: Task) -> dict:
    print(f"processing noteid: {TASK.noteid}")
    r = await servlet.GetNoteInfo(jid=ran_jid(), noteid=TASK.noteid)
    
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
def claim_task(queue: Collection, status: str = Status.TODO) -> Optional[Task]:
    assert status in Status.__dict__.values()

    TASK = queue.find_one_and_update(
        filter={"status": status},
        update={"$set": {
            "status": Status.PROCESSING,
            "claim_at": Timestamp(int(time.time()), 0),
            "update_at": Timestamp(int(time.time()), 0),
            }},
        sort=[("noteid", pymongo.ASCENDING)],
    )
    return Task(**TASK) if TASK else None


@CallTimer()
def update_task(c_notes_queue: Collection, TASK: Task, status: str):
    assert c_notes_queue.name == "notes_queue"
    assert status in Status.__dict__.values()

        
    c_notes_queue.find_one_and_update(
        filter={"_id": TASK._id},
        update={"$set": {
            "status": status,
            "update_at": Timestamp(int(time.time()), 0),
        }},
    )

async def worker(c_notes: Collection, c_notes_queue: Collection, servlet: Servlet):
    while not os.path.exists("stop"):
        # 1. claim a task
        TASK = claim_task(c_notes_queue, status=Status.TODO)
        if not TASK:
            print("no task to claim")
            await asyncio.sleep(10)
            continue

        # 2. process task
        try:
            r_json = await process_task(servlet, TASK)
        except Empty as e:
            print(e)
            update_task(c_notes_queue, TASK, Status.EMPTY)
            continue
        except Exception as e:
            print(e)
            update_task(c_notes_queue, TASK, Status.FAIL)
            continue

        # 3. update task
        insert_onte(c_notes, TASK.noteid, r_json, Status.DONE)
        print(f"DONE noteid: {TASK.noteid}, inserted", len(json.dumps(r_json, separators=(',', ':'), ensure_ascii=False))//1024, "KB payload")
        update_task(c_notes_queue, TASK, Status.DONE)

@CallTimer()
def insert_onte(c_notes: Collection, noteid: int, payload: dict|None, status: str):
    assert c_notes.name == "notes"
    assert status == Status.DONE, "目前只记录有完整 payload 的 DONE 数据"

    assert status in [Status.DONE, Status.EMPTY]
    if status == Status.DONE:
        assert payload is not None
    if status == Status.EMPTY:
        assert payload is None

    c_notes.insert_one({
        "noteid": noteid,
        "status": status,
        "payload": payload,
    })


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


    c_notes_queue: Collection = db.notes_queue
    if args.task_provider:
        create_notes_index(c_notes_queue)
        last_op = time.time()
        while not os.path.exists("stop"):
            max_noteid = find_max_noteid(c_notes_queue)
            if max_noteid >= args.end_noteid:
                print(f"max_noteid >= args.end_noteid: {max_noteid} >= {args.end_noteid}")
                break

            print(f"{datetime.datetime.now()} | will created {args.qos} TODO tasks: {max_noteid}->{max_noteid+1+args.qos}")

            init_queue(c_notes_queue, start_noteid=max_noteid+1, end_noteid=max_noteid+1+args.qos)
            t = 1 - (time.time() - last_op)
            await asyncio.sleep(t if t > 0 else 0)
            last_op = time.time()

    c_notes: Collection = db.notes
    create_notes_index(c_notes)
    count_docs_init = c_notes_queue.count_documents(filter={})
    if count_docs_init == 0:
        raise Exception("notes collection is empty, Wrong database?")



    cors = [
        worker(
            c_notes=c_notes,
            c_notes_queue=c_notes_queue,
            servlet=servlet
        )for _ in range(3)
    ]

    await asyncio.gather(*cors)

if __name__ == "__main__":
    asyncio.run(main())