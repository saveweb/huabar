

import pymongo
from pymongo.database import Database
from pymongo.collection import Collection
import pymongo.errors

def praise_ana(c_notes):
    # 从 notes 集合中统计 payload.praise 的分布情况(从大到小)
    # {"payload.praise大小": doc的个数, ...}
    # 例如: {"42864824": 10, "1233123": 12, "123": 1, ...}

    pipeline = [
        {"$group": {"_id": "$payload.praise", "count": {"$sum": 1}}},
        {"$sort": {"_id": -1}}
    ]

    result = c_notes.aggregate(pipeline)
    results = []

    for doc in result:
        print(doc)
        results.append(doc)

    with open("praise_ana.txt", "w", encoding="utf-8") as f:
        for doc in results:
            f.write(f"{doc['_id']}\t{doc['count']}\n")

def noteossurl_keyword(notes, includ="hangzhou"):
    # 从 notes 集合中统计 payload.noteossurl 字符串中包含 "hangzhou" 的文档个数
    count = notes.count_documents(
        {"payload.noteossurl": {"$regex": includ}}
        )
    print(f"payload.noteossurl 中包含 {includ} 的文档个数: {count}")
    

if __name__ == "__main__":
    m_client = pymongo.MongoClient("mongodb://[_____]")
    db: Database= m_client.huabar
    c_notes: Collection = db.notes
    
    praise_ana(c_notes)