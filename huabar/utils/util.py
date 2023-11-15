import random
import time

from .hotjids import HOT_JIDS

def gen_DEVICE_UA():
    DEVICE_UA_LIST = """\
Dalvik/2.1.0 (Linux; U; Android 9; MC401_GWL Build/PKQ1.190626.001)
Dalvik/2.1.0 (Linux; U; Android 11; RMX1851 Build/RQ3A.210805.001.A1)
Dalvik/2.1.0 (Linux; U; Android 11; RMX3151 Build/RP1A.200720.011) XDL
Dalvik/2.1.0 (Linux; U; Android 7.1.2; Easy_XL_Pro Build/NJH47F)
Dalvik/2.1.0 (Linux; U; Android Tiramisu Build/TPP2.220218.008)
Dalvik/2.1.0 (Linux; U; Android 12; Redmi K30 Build/SKQ1.210908.001)
Dalvik/2.1.0 (Linux; U; Android 9; MAR-LX2B Build/HUAWEIMAR-L22BX)
Dalvik/2.1.0 (Linux; U; Android 11.1; X96Q Build/NHG47L)
Dalvik/2.1.0 (Linux; U; Android 9; AFTKA Build/PS7279.2766N)
Dalvik/2.1.0 (Linux; U; Android 11; AGM_H3 Build/RP1A.200720.011)
Dalvik/2.1.0 (Linux; U; Android 9; Turbopad1016NEW Build/PPR1.180610.011)
Dalvik/2.1.0 (Linux; U; Android 6.0.1; FZ-N1 Build/MOB31K)
Dalvik/2.1.0 (Linux; U; Android 10; Aqua S9 Build/Q00711)
Dalvik/2.1.0 (Linux; U; Android 6.0.1; q201_9377 Build/MHC19J)
Dalvik/2.1.0 (Linux; U; Android 11; V20 Build/RP1A.200720.011)
Dalvik/2.1.0 (Linux; U; Android 11; F-51B Build/V11RD61C)
Dalvik/2.1.0 (Linux; U; Android 11; dedede Build/R100-14526.89.0)
Dalvik/2.1.0 (Linux; U; Android 11; SM-T225C Build/RP1A.200720.012)
Dalvik/2.1.0 (Linux; U; Android 9; Blaupunkt SM 05 Build/PPR1.180610.011)
Dalvik/2.1.0 (Linux; U; Android 10; A60Plus Build/QP1A.190711.020)
Dalvik/2.1.0 (Linux; U; Android 11; X98mini Build/RQ2A.210505.003)
Dalvik/2.1.0 (Linux; U; Android 12; SM-A307FN Build/SP2A.220405.004)
Dalvik/2.1.0 (Linux; U; Android 11; Pixel 2 Build/RQ3A.211001.001)
Dalvik/2.1.0 (Linux; U; Android 8.1.0; X-TIGI_V15 Build/O11019)
Dalvik/2.1.0 (Linux; U; Android 8.0.0; 601HT Build/OPR6.170623.013)
Dalvik/2.1.0 (Linux; U; Android 8.0.0; moto x4 Build/OPWS27.57-40-3-5)
Dalvik/2.1.0 (Linux; U; Android 7.0; A7 Build/NRD90M)
Dalvik/2.1.0 (Linux; U; Android 11; 2201117SG Build/RP1A.200720.011)
Dalvik/2.1.0 (Linux; U; Android 8.1.0; TAQ-70 Build/OPM8.190505.001)
Dalvik/2.1.0 (Linux; U; Android 11; K108 Build/RP1A.201005.001)
Dalvik/2.1.0 (Linux; U; Android 5.1.1; LGL61AL Build/LMY47V)
Dalvik/2.1.0 (Linux; U; Android 12; CPH2023 Build/RKQ1.211103.002)
Dalvik/2.1.0 (Linux; U; Android 12; SM-G975N Build/SP1A.210812.016)
Dalvik/2.1.0 (Linux; U; Android 11; moto g play (2021) Build/RZAS31.Q2-146-14-6-3)
Dalvik/2.1.0 (Linux; U; Android 11; W-V770 Build/RP1A.200720.011)
""".splitlines()
    return DEVICE_UA_LIST[int(time.time())%(len(DEVICE_UA_LIST)-1)]

default_headers = {
    'Content-Type': 'application/octet-stream',
    'uid': 'newuser',
    'logincode': '-1',
    'hbtoken': '',
    'imei': '',
    'os': 'android',
    'vcode': '255',
    'User-Agent': gen_DEVICE_UA(),
}

def auto_headers():
    headers = default_headers.copy()
    headers['User-Agent'] = gen_DEVICE_UA()
    return headers

def ran_jid():
    return random.choice(HOT_JIDS)