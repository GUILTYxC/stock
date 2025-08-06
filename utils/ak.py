import sqlite3
import time
import pandas as pd
import akshare as ak
from datetime import datetime, timedelta

import requests
from tqdm import tqdm

DB_PATH = '../ma_cache.db'
DEVIATION = 7


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


def get_ma_xl(symbol, days_range=300):
    # end_date是今天，start_date是往前300天，同时计算ma50和ma200
    today = datetime.today().strftime('%Y%m%d')

    start_date = (datetime.strptime(today, '%Y%m%d') - timedelta(days=days_range)).strftime('%Y%m%d')

    if symbol.startswith('6'):
        full_code = 'sh' + symbol
    elif symbol.startswith('8'):
        full_code = 'bj' + symbol
    else:
        full_code = 'sz' + symbol
    try:
        result_df = ak.stock_zh_a_daily(symbol=full_code, start_date=start_date, end_date=today, adjust="qfq")
    except requests.exceptions.ConnectionError:
        # 等待10秒
        print("请求错误，等待10秒后重试")
        time.sleep(10)
        result_df = ak.stock_zh_a_daily(symbol=full_code, start_date=start_date, end_date=today, adjust="qfq")
    except Exception:
        print(symbol + "数据处理失败，已跳过")
        return {
            'ma50': None,
            'ma200': None,
            'current': None
        }
    if len(result_df) >= 50:
        ma50 = result_df['close'].iloc[-50:].mean()
    else:
        ma50 = None  # 或其他处理方式
    if len(result_df) >= 200:
        ma200 = result_df['close'].iloc[-200:].mean()
    else:
        ma200 = None

    result = {
        'ma50': ma50,
        'ma200': ma200,
        'current': result_df['close'].iloc[-1]
    }

    return result


# 检查当前股价是否低于ma50某个百分比
def check(ma, deviation):
    if ma['current'] and ma['ma50']:
        ratio = ma['current'] / ma['ma50']
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
            'CREATE TABLE IF NOT EXISTS stock_ma (code TEXT ,name TEXT, ma50 REAL,price REAL, date TEXT ,deviation REAL, PRIMARY KEY (code, date))')


def profcess_all_stock():
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
        ma_info = get_ma_xl(code, 100)
        check_result = check(ma_info, DEVIATION)
        if check_result:
            deviation = check_result['deviation']
            if check_result['check']:
                checked_stock_list.append({
                    'code': code,
                    'name': name,
                    'ma50': ma_info['ma50'],
                    'price': ma_info['current'],
                    'deviation': deviation
                })

        else:
            deviation = None
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                'REPLACE INTO stock_ma (code, name,ma50,price, date,deviation) VALUES (?, ?, ?, ?, ?, ?)',
                (code, name, ma_info['ma50'], ma_info['current'], datetime.today().strftime('%Y%m%d'), deviation,)
            )
        # 等待0.5秒再请求
        # time.sleep(0.1)
    return checked_stock_list


def get_dip_stock():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT code, name, ma50, price, deviation FROM stock_ma WHERE  date = ?",
                       (datetime.today().strftime('%Y%m%d'),))
        stock_list = cursor.fetchall()
        # 过滤deviation小于-7的
    if not stock_list or len(stock_list) == 0:
        return profcess_all_stock()
    result = [{
        'code': stock[0],
        'name': stock[1],
        'ma50': stock[2],
        'price': stock[3],
        'deviation': stock[4]
    } for stock in stock_list if stock[4] < -DEVIATION]
    return result


if __name__ == '__main__':
    print(profcess_all_stock())
    # print(get_ma_xl('836247', 100))
