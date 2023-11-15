import requests as rq
import json

def main():
    ss = rq.Session()
    # http://ngx.haowanlab.com/RegisterDemo1/servlet/GetAppreciation?actioninfo=4&appreid=0
    url = "http://ngx.haowanlab.com/RegisterDemo1/servlet/GetAppreciation"
    params = {
        "actioninfo": 4,
        "appreid": 0
    }
    jids = set()
    while True:
        try:
            print(params["appreid"])
            r = ss.get(url, params=params)
            j = r.json()
            relist = j["relist"]
            for item in relist:
                jids.add(item["jid"])
                params["appreid"] = item["appreid"]
        except:
            break
    print(len(jids))
    with open("hotjids.txt", "w") as f:
        f.write("\n".join(jids))

if __name__ == "__main__":
    main()