URL_BAD = False

def hangzhou_internalable(url: str):
    if 'oss-cn-hangzhou.' in url:
        return True
    else:
        return False


def hangzhou_internal_url(url: str):
    if 'oss-cn-hangzhou' in url:
        return url.replace('oss-cn-hangzhou.', 'oss-cn-hangzhou-internal.', 1)
    
    raise ValueError('Unknown hangzhou url: ' + url)

def get_domain(url: str):
    if url.startswith('http://'):
        return url[7:].split('/', 1)[0]
    elif url.startswith('https://'):
        return url[8:].split('/', 1)[0]
    else:
        raise ValueError('Unknown url: ' + url)

def get_key(url: str):
    # http://{domain}/{key}
    if url.startswith('http://'):
        return url[7:].split('/', 1)[1]
    elif url.startswith('https://'):
        return url[8:].split('/', 1)[1]
    else:
        raise ValueError('Unknown url: ' + url)

def std_url(url: str):
    # haowanlab 七牛杭州
    if url.startswith('http://qncdn.haowanlab.com/'):
        stdd = url.replace('http://qncdn.haowanlab.com/', 'http://haowanlab.qiniudn.com/', 1)
        return stdd
    elif url.startswith('http://haowanlab.qiniudn.com/'): # 标准形式
        return url

    # haowanlab 阿里杭州
    elif url.startswith('http://haowanlab.oss-cn-hangzhou.aliyuncs.com/'): # 标准形式
        return url
    elif url.startswith('https://haowanlab.oss-cn-hangzhou.aliyuncs.com/'):
        # https 形式的只有几十个
        stdd = url.replace('https://', 'http://', 1) if url.startswith('https://') else url
        # print(stdd)
        return stdd
    elif url.startswith('http://oss-cn-hangzhou.aliyuncs.com/haowanlab/'):
        stdd = url.replace('http://oss-cn-hangzhou.aliyuncs.com/haowanlab/', 'http://haowanlab.oss-cn-hangzhou.aliyuncs.com/', 1)
        
        return stdd
    elif url.startswith('http://haowanlab.oss.aliyuncs.com/'):
        stdd = url.replace('http://haowanlab.oss.aliyuncs.com/', 'http://haowanlab.oss-cn-hangzhou.aliyuncs.com/', 1)
        return stdd

    # 少数资源
    elif url.startswith('http://huaba-operate.oss-cn-hangzhou.aliyuncs.com/'):
        return url
    elif url.startswith('http://notecontent.oss-cn-hangzhou.aliyuncs.com/'):
        return url

    # BAD URLs
    elif url.startswith('http://imax.vmall.com/'):
        return URL_BAD
    elif url == " ?x-oss-process=style/picmax":
        return URL_BAD
    elif url == '"(null)"' or url == '(null)':
        return URL_BAD

    else:
        raise ValueError('Unknown URL: ' + url)
