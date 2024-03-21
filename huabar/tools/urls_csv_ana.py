import csv

from huabar.utils.urls import get_domain, hangzhou_internal_url, hangzhou_internalable, std_url

{"noteid": 123, "praise": 123, "url": "", "status": "TODO"}

# index -> domain -> 

def read_csv():
    good_std_urls = set()
    domain_urls_count = {}
    notecontent_f = open('notecontent.oss-cn-hangzhou.aliyuncs.com.urls.list', 'w')
    huaba_operate_f = open('huaba-operate.oss-cn-hangzhou.aliyuncs.com.urls.list', 'w')
    with open('huabar_works.oss_pics.urls.raw.csv', 'r') as csvfile:
        '''
noteid,payload.praise,payload.noteossurl,payload.original_url
19220955,97,http://qncdn.haowanlab.com/fdc1f9015d5dc32f373e2780b0b2d1f8,http://qncdn.haowanlab.com/83ff341c8a9bb276ef4bc7132007c65a
19220954,0,http://qncdn.haowanlab.com/655ee58458038842c10f6a2ab6184bdd.data,http://qncdn.haowanlab.com/04f87a782584856de482e88efeab0d42.png
        '''

        reader = csv.reader(csvfile, strict=True)
        reader.__next__()
        for row in reader:
            noteid = row[0]
            noteossurl = row[1]
            original_url = row[2]
            std_ossurl = std_url(noteossurl) if noteossurl else None
            std_original_url = std_url(original_url) if original_url else None

            for url in [std_ossurl, std_original_url]:
                if not url:
                    continue
                assert isinstance(url, str)
                if url in good_std_urls:
                    continue
                good_std_urls.add(url)
                domain = get_domain(url)
                if domain not in domain_urls_count:
                    domain_urls_count[domain] = 0
                domain_urls_count[domain] += 1

                if domain == 'notecontent.oss-cn-hangzhou.aliyuncs.com':
                    notecontent_f.write(url + '\n')
                elif domain == 'huaba-operate.oss-cn-hangzhou.aliyuncs.com':
                    huaba_operate_f.write(url + '\n')

    
    print('good_urls:', len(good_std_urls))
    print('domain_urls_count:', domain_urls_count)
    # print('internal_urls:', len([url for url in good_std_urls if hangzhou_internalable(url)]))
    # with open('huabar_works.oss_pics.urls.good.praise>=3.list', 'w') as f:
    #     for url in good_urls:
    #         f.write(url + '\n')



if __name__ == '__main__':
    read_csv()

