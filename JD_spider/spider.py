import json
import requests
from lxml import etree
import csv
from argparse import ArgumentParser
from datetime import datetime
from tqdm import tqdm
import pandas as pd
import sqlalchemy
import time
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler


parser = ArgumentParser(description=__doc__)
parser.add_argument('--page', type=int, default=2, help="number of page")
parser.add_argument('--host', type=str, default='localhost', help='mysql host')
parser.add_argument('--database', type=str, default='dod_formarketing', help='database')
parser.add_argument('--table', type=str, default='pricefromjd', help='table name')
parser.add_argument('--user', type=str, default='root', help='user name')
parser.add_argument('--password', type=str, default='tao04111065', help='password')

args = parser.parse_args()
print(' ' * 50 + 'Options')
print('*' * 100)
for key, value in vars(args).items():
    print(f'{key}:\t{value}')
print('*' * 100)


def get_good_urls(page):
    p = page * 2 - 1
    good_urls = []
    for it in range(p, p + 2):
        # url = "https://search.jd.com/Search?keyword=" + keyword + "&enc=utf-8&qrst=1&rt=1&stop=1&vt=1&stock=1&page=" + str(
        #     it) + "&s=" + str(1 + (it - 1) * 30) + "&click=0&scrolling=y"
        url = f'https://search.jd.com/Search?keyword=%E7%94%B5%E8%A7%86&qrst=1&wq=%E7%94%B5%E8%A7%86&stock=1&page={it}' \
              f'&s={str(1 + (it - 1) * 30)}&click=0'
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.100 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30)
        assert response.status_code == 200
        response.encoding = response.apparent_encoding
        html = etree.HTML(response.text)
        for j in html.xpath('//*[@id="J_goodsList"]/ul/li/div/div[1]/a/@href'):
            if "https" in j:
                good_urls.append(j)
            else:
                good_urls.append("https:" + j)

    data = []
    for i in tqdm(good_urls):
        try:
            data.append(get_information(i))
            # time.sleep(10)
        except Exception as e:
            print(e)

    filed_word = ['时间','店铺名称','商品名称','价格','屏幕尺寸','分辨率','链接']
    key_words = data[0].keys()
    values = [item.values() for item in data]
    data_df = pd.DataFrame(values, columns=key_words)
    data_df = data_df.reindex(columns=filed_word)
    save_data(data_df)

    return good_urls


def get_price(sid):
    price_url = "https://p.3.cn/prices/mgets?skuIds=" + sid
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/80.0.3987.100 Safari/537.36 '
    }
    response = requests.get(price_url, headers=headers, timeout=30)
    response.encoding = response.apparent_encoding
    jsons = json.loads(response.text[0:-1])
    price = jsons[0]['p']
    if price == "-1.00":
        return "商品已售完"
    else:
        return price


def get_information(url):
    temp = url.split('/')[-1]
    sid = temp.split('.')[0]
    headers = {
        'authority': 'list.jd.com',
        'method': 'GET',
        'path': '/' + url.split('/')[-1],
        'scheme': 'https',
        'accept': '/list.html?cat=9987,653,655&page=1&sort=sort_rank_asc&trans=1&JL=6_0_0&callback=jQuery8321329&md=9&l=jdlist&go=0',
        'accept-encoding': 'gzip, deflate, br',
        'accept-encoding': 'gzip, deflate, br',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.100 Safari/537.36'
    }
    response = requests.get(url, headers=headers, timeout=30)
    timestamp = datetime.now()
    if response.status_code != 200:
        print('爬取失败%s' % timestamp)
        return

    response.encoding = response.apparent_encoding
    html = etree.HTML(response.text)
    with open('temp.html', 'w') as f:
        f.write(response.text)
    goodsname = html.xpath('//*[@id="detail"]/div[2]/div[1]/div[1]/ul[2]/li[1]/text()')
    good_store = html.xpath('//*[@id="crumb-wrap"]/div/div[2]/div[2]/div[1]/div/a/text()')
    screen_size = html.xpath('//*[@id="detail"]/div[2]/div[1]/div[1]/ul[2]/li[5]/text()')
    resolution_ratio = html.xpath('//*[@id="detail"]/div[2]/div[1]/div[1]/ul[2]/li[7]/text()')

    # 字符串处理
    good_store = good_store[0]
    good_price = get_price(sid)
    good_name = str(goodsname[0]).split("：")[1]
    resolution_ratio = resolution_ratio[0].strip().split(':')[-1]
    screen_size = screen_size[0].strip().split(':')[-1]

    info = {
        '店铺名称': good_store,
        '商品名称': good_name,
        '价格': good_price,
        '屏幕尺寸': screen_size,
        '分辨率': resolution_ratio,
        '链接': url,
        '时间': timestamp
    }
    return info


def save_data(data_df):
    assert isinstance(data_df, pd.DataFrame)
    # 建立数据库连接
    engine = sqlalchemy.create_engine(f'mysql+pymysql://{args.user}:{args.password}@{args.host}/{args.database}?charset=utf8')
    data_df.drop_duplicates(inplace=True)
    data_df.to_sql(args.table, engine, index=False, if_exists='append')
    print('数据已存入数据库！')

def main():
    for i in range(1, args.page + 1):
        get_good_urls(i)


if __name__ == "__main__":
    # schedluler1 = BlockingScheduler()
    # schedluler1.add_job(main, trigger='cron', hour='17,20,22', minute='5', id='job1', max_instances=1)
    # schedluler1.start()
    main()
