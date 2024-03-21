import requests
import os

def download_course(ss, data: dict):
    courseid = data['courseid']
    # os.makedirs(f'course/{courseid}', exist_ok=True)
    if 'url' in data:
        print(data['url'])
    for i in data['catalog']:
        videourl = i['videourl']
        picurl = i['picurl']
        print(videourl)
        print(picurl)

def mian():
    ss = requests.session()
    api = "http://s.haowanlab.com:8900/RegisterDemo1/servlet/Course?jid=f3bhs-0@zhizhiyaya.com/HuaLiao&reqtype=courseinfo&courseid="
    for i in range(0, 100):
        r = ss.get(f'{api}{i}')
        r_json = r.json()
        if r_json['catalog']:
            # print(r_json['catalog'])
            download_course(ss, r_json)
        
if __name__ == '__main__':
    mian()