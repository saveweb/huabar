import requests
import httpx
import json

from huabar.ngx.define import NGX_HTTPS_PREFIX, NGX_HTTP_PREFIX

class Servlet:
    client: httpx.AsyncClient
    ss: requests.Session

    def __init__(self, client: httpx.AsyncClient = None, ss: requests.Session = None):
        self.client = client
        self.ss = ss

    class actiontype:
        follow = 1
        unfollow = 2

        delete_post = 7

    async def GetNoteInfo(self, jid: str, noteid: int, reqtype: int = 1):
        """
        reqtype: 1 # TODO: 不知道是什么

        return:

            ["noteossurl"] 绘画过程数据，3D 没有，被删除的没有
                http://notecontent.oss-cn-hangzhou.aliyuncs.com/{noteid} # 早期
                http://qncdn.haowanlab.com/19116313b2bbfd63948971e593f60c51
                http://imax.vmall.com/resource/20140515/40d6cf0e-a8f7-40a9-b8be-f6637393eb8a.png?x-oss-process=style/picmax # 如 199197 的远古，已失效
            ["3dcommodity"] 3D 模型信息
                {"url": "http://ngx.haowanlab.com/3dshop/index.html?from=notedetail#/ModShopDetail/s_c_37","ctext": "同款模型"}
            ["elementid"] 3D 作品的 elementid
                ch_s_c_37j8mo3t5601587443400720022803835
                查看 3D 需要：
                    http://ngx.haowanlab.com/3dshop/3dh5_share.html?eid={elementid}&jid={作者的jid}
                    不加作者 jid 的话没有音乐(?)
            ["original_url"] 为原图：
                http://haowanlab.qiniudn.com/964206cba38d4cb7ba6411dac146bfba.png
                http://qncdn.haowanlab.com/d0b42ecac71f7deee881e08db1554887
                http://huaba-operate.oss-cn-hangzhou.aliyuncs.com/deletepic.png # note 被删除，远古帖子
            ["voice"] 为音乐:
                http://qncdn.haowanlab.com/3cb2ea432cfd1d34943255f2308be23f.mp3
        """
        API = NGX_HTTP_PREFIX + "/RegisterDemo1/servlet/GetNoteInfo"
        params = {
            'noteid': noteid,
            'jid': jid,
            'reqtype': reqtype,
        }
        r = await self.client.get(API, params=params)
        return r

    def GetComList(self, noteid: int, visjid: str, visid: int = 0, reqtype: str = "root"):
        """
        noteid: ....
        visjid: 看起来和 jid 一样就行？
        visiid: 0 # TODO: 不知道
        reqtype: "root" # TODO: 不知道
        """

        API = NGX_HTTP_PREFIX + "/RegisterDemo1/servlet/GetComList"
        params = {
            'noteid': noteid,
            'visid': visid,
            'visjid': visjid,
            'reqtype': reqtype,
        }
        r = self.ss.get(API, params=params)
        return r


    # TODO: 不知道是啥
    def GetInvcode(self, jid: str):
        """
        返回:
            {"invcode": ...int...}
        """
        API = NGX_HTTP_PREFIX + "/RegisterDemo1/servlet/GetInvcode"
        params = {
            'jid': jid,
        }
        r = self.ss.get(API, params=params)
        return r



    # 最新作品
    def GetLatestNote(self, jid: str, reqtype: int = 1, noteid: int = 0, page: int = 1):
        """
        reqtype: 1 全部作品, 2 原创作品
        noteid: 0 # 初始 0 ，用 relist 中最小的 "createtime" 作为下一次请求
        page: 1 # 初始 1 ，翻页 +1

        return:
            ...relist: [
                {"headline":"柳叶笔摸鱼","havevoice":"n","nickname":"铁铁子","ismember":0,"url":"http://qncdn.haowanlab.com/35e55a13186ebe51593eb3a2a638a9ba-mid800","createtime":1699886185,"noteid":19193872,"anon":1,"huabacoin":0,"notetype":3,"jid":"l8kl4yi7-0@zhizhiyaya.com/HuaLiao","praise":2,"faceurl":"http://qncdn.haowanlab.com/d405013a46d73ab4b6244d9e117fd9df","aspectratio":1.0}
        """
        API = NGX_HTTP_PREFIX + "/RegisterDemo1/servlet/GetLatestNote"
        params = {
            'jid': jid,
            'reqtype': reqtype,
            'noteid': noteid,
            'page': page,
        }
        r = self.ss.get(API, params=params)
        return r
    # import datetime
    # import time
    # print(
    #     [item["createtime"] for item in GetLatestNote(ss, MY_JID).json()["relist"]]
    # )
    # exit()


    # 最新绘本
    def NewGetBookList(self, jid, vsjid, reqid: int = 0, actiontype: int = 3, page: int = 1):
        """
        vsjid 和 jid 一致
        *reqid: 0 # 初始 0 ，用 relist[-1].bookid 作为下一次请求
        *actiontype: 3 # TODO: 不知道是什么
        page: 1 # 初始 1 ，翻页 +1

        """
        """actiontype: 1
        {"numberstr":"12","number":12,"notenumstr":"79,949","relist":[],"notenum":79949}
        """
        """
        actiontype: 3   
        {
        "numberstr": "12",
        "number": 12,
        "notenumstr": "79,949",
        "relist": [
                {
                "coverid": 19183858,
                "title": "银魂",
                "bookid": 79949, # NOTE: 这就是 reid
                "nickname": "怀远微素",
                "ismember": 0,
                "pagenum": 17,
                "bookstatus": 1,
                "praise": 34,
                "jid": "nt2iondw-0@zhizhiyaya.com/HuaLiao",
                "aspectratio": 1,
                "url": "http://qncdn.haowanlab.com/1a4e66d75839e3f2fc6306cd88e3d087-mid800",
                "faceurl": "http://qncdn.haowanlab.com/c4902d801dcbbb4e44581c4dac77efa6",
                "user_ex": {
                "remark": "",
                "frameid": 29
                },
                ......
            ],
            "notenum": 79949
        },
        """
        API = NGX_HTTP_PREFIX + "/RegisterDemo1/servlet/NewGetBookList"
        params = {
            "jid": jid,
            "vsjid": vsjid,
            "reqid": reqid,
            "actiontype": actiontype,
            "page": page,
        }
        r = self.ss.get(API, params=params)
        return r

    def GetAppreciation(self, jid, actioninfo: int = 4, appreid :int=0, page:int =0):
        """
        actioninfo: 4 # TODO:
        appreid: 0 # 初始 0 ，用 relist[-1].appreid 作为下一次请求
        page: 0 # 分页，后端应该不看 page 而是 appreid 了，没用了

        返回:
            {
                "numberstr": "0",
                "number": 0,
                "notenumstr": "19,193,888",
                "relist": [
                    {
                        "headline": "白",
                        "havevoice": "n",
                        "nickname": "银秋·梦雨",
                        "ismember": 0,
                        "pagenum": 0,
                        "url": "http://qncdn.haowanlab.com/ef35854943293ef9b8a5a7d0d8e0c9bb-mid800",
                        "user_ex": {
                            "remark": "",
                            "frameid": 0
                        },
                        "noteid": 19185831,
                        "bookid": 0,
                        "appreid": 966613,
                        "huabacoin": 0,
                        "notetype": 3,
                        "originalratio": 0.625,
                        "jid": "3l24m538-0@zhizhiyaya.com/HuaLiao",
                        "praise": 1584,
                        "faceurl": "http://qncdn.haowanlab.com/05d12dc501c568f7803f0b13c3d94d2e",
                        "aspectratio": 0.625
                    },
                    ...省略...
                ],
                "notenum": 19193888
            }
        """
        API = NGX_HTTP_PREFIX + "/RegisterDemo1/servlet/GetAppreciation"
        params = {
            "jid": jid,
            "actioninfo": actioninfo,
            "appreid": appreid,
            "page": page
        }
        r = self.ss.get(API, params=params)
        return r

    def Login(self, jid: str, passwd: str,os: str = "android", version: str = "7.6.8"):
        """
        {MY_JID}
        """
        API = NGX_HTTPS_PREFIX + "/RegisterDemo1/servlet/Login"
        params = {
            'os': os,
            'jid': jid,
            'passwd': passwd,
            'version': version,
        }
        r = self.ss.get(API, params=params)
        return r

    # 签到
    def Signin(self, jid: str):
        '''
        成功：
            {"status":"1","awards":[{"values":1,"parameter":"ExpAward","remark":"经验值奖励1"},{"values":20,"parameter":"CreditAward","remark":"积分奖励20"}],"jid":"......"}
        失败:
            {"status":"2","awards":[],"jid":"......."}
        '''
        
        API = NGX_HTTPS_PREFIX + '/RegisterDemo1/servlet/Signin'
        r_data = {
            'jid': jid,
        }
        r = self.ss.post(API, json=r_data)
        return r
    # print(
    #     Signin(ss, MY_JID).text
    # )

    # 登录
    def GetJIDByTel(self, tel: str, passwd: str, version: str = '7.6.8', imei: str = ''):
        """
        tel: 手机号
        passwd: 密码
        version: 版本号
        
        return:
            成功:
                {"status":1,"logincode":17,"jid":"用户jid","hbtoken":".........."} # ... 为省略
            失败:
                {"status":3,"logincode":115944,"jid":"","hbtoken":"qwerqwer"} # qwer 不是我随便打的，它确实返回 qwerqewr ……
        """
        API = NGX_HTTPS_PREFIX + '/RegisterDemo1/servlet/GetJIDByTel'
        params = {
            'tel': tel,
            'passwd': passwd,
            'version': version,
            'imei': imei,
        }
        r = self.ss.get(API, params=params)
        return r


    def SmallBusiness(self, jid: str, reqtype: str):
        """
        TODO: 不知道这个 API 干嘛的

        reqtype:
            "isSign"
            "hasTaskAward"
            "live_status"
            "get_face_famelist"
        返回:
            {"reqtype":"isSign","status":"n"}
            {"reqtype":"hasTaskAward","status":"n"}
            {"live_time":"画点可爱的","reqtype":"live_status","live_status":"end","live_url":"https://live.bilibili.com/h5/535017"}
            {一大坨不知道什么玩意，有很多用户的 jid}
        """
        API = NGX_HTTPS_PREFIX + '/RegisterDemo1/servlet/SmallBusiness'
        data = {
            'jid': jid,
            'reqtype': reqtype,
        }
        r = self.ss.post(API, data=json.dumps(data,separators=(',', ':')))
        return r

    # print(
    #     "\n".join([
    #         SmallBusiness(ss, MY_JID, "isSign").text,
    #         SmallBusiness(ss, MY_JID, "hasTaskAward").text,
    #         SmallBusiness(ss, MY_JID, "live_status").text,
    #         SmallBusiness(ss, MY_JID, "get_face_famelist").text,
    #     ])
    # )

    def fllow(self, jid: str, vsjid: str, actiontype: int|str, actioninfo: str):
        """
        jid: 你的 jid
        vsjid: 你想要关注的用户的 jid
        actiontype: 1 follow, 2 unfollow
        actioninfo: 被关注用户的“{actioninfo}关注了你”消息提醒

        成功:
            {"vsjid":"......","result":1,"requestid":".........."}
        失败:
            {"vsjid":"......","result":2,"requestid":".........."}
        """
        API = NGX_HTTPS_PREFIX + "/RegisterDemo1/servlet/ActionUser"
        params = {
            'jid': jid, # me
            'vsjid': vsjid, # target
            'actiontype': actiontype,
            'actioninfo': actioninfo,
        }
        r = self.ss.get(API, params=params)
        return r


    def create_post(self, jid: str,forumid: int, headline: str, ctext: str, attach: list, at: list):
        """
        
        forumid: 论坛id
        headline: 标题
        ctext: 内容
        attach: #TODO: 附件(?)
        at: #TODO: @的人(?)
        """
        API = NGX_HTTPS_PREFIX + "/RegisterDemo1/servlet/CreatePost"
        j_data = {
            'jid': jid,
            'forumid': forumid,
            'headline': headline, #'Hello World'
            'ctext': ctext, # '第一个帖子'
            'attach': attach, #[]
            'at': at, # []
        }
        r = self.ss.post(API, json=j_data)
        print(r.text)



    # 删帖子
    def ForumOpt(self, jid: str, subtype:int, postid: int|str, actiontype: int, operate: int):
        """
        postid: 帖子 id
        subtype:
            8 # TODO
        actiontype:
            7 # TODO
        operate:
            0 # TODO
        """
        API = NGX_HTTPS_PREFIX + "/RegisterDemo1/servlet/ForumOpt"
        params = {
            'jid': jid,
            'subtype': subtype,
            'postid': postid,
            'actiontype': actiontype,
            'operate': operate,
        }
        r = self.ss.get(API, params=params)
        print(r.text)
