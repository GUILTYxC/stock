import os
import sqlite3
import time

import numpy as np
import pandas as pd
import akshare as ak
from datetime import datetime, timedelta

import requests
from tqdm import tqdm

# 获取当前文件（ak.py）所在目录
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# 数据库在项目根目录下
DB_PATH = os.path.join(CURRENT_DIR, '..', 'ma_cache.db')
DEVIATION = 6


def get_ma(symbol, days_range=300):
    # end_date是今天，start_date是往前300天，同时计算ma50和ma200
    today = datetime.today().strftime('%Y%m%d')

    start_date = (datetime.strptime(today, '%Y%m%d') - timedelta(days=days_range)).strftime('%Y%m%d')

    try:
        result_df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=today,
                                       adjust="qfq")
    except requests.exceptions.ConnectionError:
        # 等待10秒
        print("请求错误，等待10秒后重试")
        time.sleep(10)
        result_df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=today,
                                       adjust="qfq")
    if len(result_df) >= 50:
        ma50 = result_df['收盘'].iloc[-50:].mean()
    else:
        ma50 = None  # 或其他处理方式
    if len(result_df) >= 200:
        ma200 = result_df['收盘'].iloc[-200:].mean()
    else:
        ma200 = None

    result = {
        'ma50': ma50,
        'ma200': ma200,
        'current': result_df['收盘'].iloc[-1]
    }

    return result


def get_ma_xl(symbol, end_date, days_range=300):
    # end_date是今天，start_date是往前300天，同时计算ma50和ma200
    today = datetime.today().strftime('%Y%m%d')

    start_date = (datetime.strptime(today, '%Y%m%d') - timedelta(days=days_range)).strftime('%Y%m%d')

    if symbol.startswith('6'):
        full_code = 'sh' + symbol
    elif symbol.startswith('8') or symbol.startswith('9'):
        full_code = 'bj' + symbol
    else:
        full_code = 'sz' + symbol
    try:
        result_df = ak.stock_zh_a_daily(symbol=full_code, start_date=start_date, end_date=end_date, adjust="qfq")
    except requests.exceptions.ConnectionError:
        # 等待10秒
        print("请求错误，等待10秒后重试")
        time.sleep(10)
        result_df = ak.stock_zh_a_daily(symbol=full_code, start_date=start_date, end_date=end_date, adjust="qfq")
    except Exception:
        print(symbol + "数据处理失败，已跳过")
        return {
            'ma50': None,
            'ma200': None,
            'wma50': None,
            'ema50': None,
            'current': None
        }
    if len(result_df) >= 50:
        ma50 = round(result_df['close'].iloc[-50:].mean(), 2)
        wma50 = round(calculate_wma50(result_df), 2)
        ema50 = round(result_df['close'].ewm(span=50, adjust=False).mean().iloc[-1], 2)
    else:
        ma50 = None  # 或其他处理方式
        wma50 = None
        ema50 = None
    if len(result_df) >= 200:
        ma200 = round(result_df['close'].iloc[-200:].mean(), 2)
    else:
        ma200 = None

    result = {
        'ma50': ma50,
        'wma50': wma50,
        'ema50': ema50,
        'ma200': ma200,
        'current': result_df['close'].iloc[-1]
    }

    return result


def calculate_wma50(result_df):
    # 获取最近50天的收盘价
    recent_closes = result_df['close'].iloc[-50:]

    # 创建权重数组，最新数据权重最高
    weights = range(1, 51)  # 1到50的权重

    # 计算加权移动平均线
    wma50 = (recent_closes * weights).sum() / sum(weights)

    return wma50


def calculate_wma50_2(result_df):
    # 假设 result_df 已经按时间升序排列（最老在前，最新在后）
    prices_50 = result_df['close'].iloc[-50:]  # 最近50天，从旧到新

    weights = np.arange(1, 51)  # 权重从1到50，越近越高

    wma50 = np.average(prices_50, weights=weights)

    return wma50


# 检查当前股价是否低于ma50某个百分比
def check(ma, deviation):
    if ma['current'] and ma['ema50']:
        ratio = ma['current'] / ma['ema50']
        # 如果当前价格比ma50低7%
        checked = False
        if ratio <= (100 - deviation) / 100:
            checked = True
        return {
            'check': checked,
            'deviation': round((ratio - 1) * 100, 2)
        }
    return None


def init_stock_info():
    stock_base_infos = ak.stock_zh_a_spot_em()
    data = list(stock_base_infos[['代码', '名称']].itertuples(index=False, name=None))
    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany(
            'REPLACE INTO stock_list (code, name) VALUES (?, ?)',
            data
        )
        # conn.commit()
    print("成功初始化股票基本信息数据")


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('CREATE TABLE IF NOT EXISTS stock_list (code TEXT PRIMARY KEY, name TEXT)')
        conn.execute(
            'CREATE TABLE IF NOT EXISTS stock_ma (code TEXT ,name TEXT, ma50 REAL,price REAL, date TEXT ,deviation REAL,'
            'wma50 REAL, ema50 REAL,PERatio REAL, PRIMARY KEY (code, date))')


def profcess_all_stock(date):
    # 先查所有股票的实时行情，获取市盈率，构建code，市盈率的map
    stock_base_infos = ak.stock_zh_a_spot_em()
    stock_pe_map = {
        stock_base_info[1]: stock_base_info[15]
        for stock_base_info in stock_base_infos.itertuples(index=False, name=None)
    }
    with sqlite3.connect(DB_PATH) as conn:
        # 创建游标对象（用于执行查询）
        cursor = conn.cursor()
        # 执行查询语句
        cursor.execute("SELECT code, name FROM stock_list")
        # 获取所有结果（返回元组列表，格式：[(code1, name1), (code2, name2), ...]）
        stock_list = cursor.fetchall()
        # 去除科创板和创业板
        stock_list = [stock for stock in stock_list if not stock[0].startswith('30') and not stock[0].startswith('688')]
        # stock_list = stock_list[0:10]
    checked_stock_list = []
    for code, name in tqdm(stock_list, desc="处理进度", unit="只股票"):
        ma_info = get_ma_xl(code, date, 100)
        check_result = check(ma_info, DEVIATION)
        if check_result:
            deviation = check_result['deviation']
            if check_result['check']:
                checked_stock_list.append({
                    'code': code,
                    'name': name,
                    'ma50': ma_info['ma50'],
                    'wma50': ma_info['wma50'],
                    'ema50': ma_info['ema50'],
                    'price': ma_info['current'],
                    'deviation': deviation,
                    'PERatio': stock_pe_map[code]
                })

        else:
            deviation = None
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                'REPLACE INTO stock_ma (code, name,ma50,wma50,ema50,price, date,deviation,PERatio) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (code, name, ma_info['ma50'], ma_info['wma50'], ma_info['ema50'], ma_info['current'],
                 date, deviation, stock_pe_map[code])
            )
        # 等待0.5秒再请求
        # time.sleep(0.1)
    return checked_stock_list


def get_dip_stock():
    # 如果当前时间是下午四点半以后，date是今天，否则是昨天
    # 获取当前时间
    now = datetime.now()

    # 设定下午四点半的时间点
    cutoff_time = now.replace(hour=16, minute=30, second=0, microsecond=0)

    # 判断日期
    if now >= cutoff_time:
        # 下午四点半及以后，使用今天的日期
        target_date = now
    else:
        # 下午四点半之前，使用昨天的日期
        target_date = now - timedelta(days=1)

    # 格式化为YYYYMMDD形式
    formatted_date = target_date.strftime("%Y%m%d")

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT code, name, ma50,wma50,ema50, price, deviation,PERatio FROM stock_ma WHERE  date = ? ORDER BY deviation,PERatio ",
                       (formatted_date,))
        stock_list = cursor.fetchall()
        # 过滤deviation小于-7的
    if not stock_list or len(stock_list) == 0:
        return profcess_all_stock(formatted_date)
    result = [{
        'code': stock[0],
        'name': stock[1],
        'ma50': stock[2],
        'wma50': stock[3],
        'ema50': stock[4],
        'price': stock[5],
        'deviation': stock[6],
        'PERatio': stock[7]
    } for stock in stock_list if stock[6] and stock[6] < -DEVIATION and 'ST' not in stock[1] and 0 < stock[7] < 100]
    return result


if __name__ == '__main__':
    # print(get_dip_stock())
    # print(get_ma_xl('836247', 100))
    # print(get_ma_xl('002847', 100))
    # stock_base_infos = ak.stock_zh_a_spot_em()
    # stock_pe_map = {
    #     stock_base_info[1]: stock_base_info[15]
    #     for stock_base_info in stock_base_infos.itertuples(index=False, name=None)
    # }
    # print(stock_pe_map)
    print(profcess_all_stock())
