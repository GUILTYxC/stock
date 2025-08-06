# utils/price_fetcher.py
import requests

def get_price(code):
    try:
        url = f"http://hq.sinajs.cn/list={code}"
        resp = requests.get(url, timeout=5)
        resp.encoding = 'gbk'
        data = resp.text.split('"')[1].split(',')
        return float(data[3])
    except:
        return None
