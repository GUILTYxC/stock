# utils/ma_calculator.py
import datetime
import os
import sqlite3
import time

import pandas as pd

DB_PATH = 'ma_cache.db'
CSV_DIR = './data/history/'

os.makedirs(CSV_DIR, exist_ok=True)

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute('CREATE TABLE IF NOT EXISTS ma50 (code TEXT PRIMARY KEY, value REAL, updated_at TEXT)')
        conn.execute('CREATE TABLE IF NOT EXISTS stock_prices  (code TEXT,date TEXT,close REAL,PRIMARY KEY (code, date))')






# B2507758-865E-42C2-A69C-56460D25D6E0
def get_ma50(code):
    today = datetime.date.today().isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute('SELECT value, updated_at FROM ma50 WHERE code=?', (code,))
        row = cursor.fetchone()
        if row and row[1] == today:
            return row[0]  # 缓存命中并未过期

    csv_path = os.path.join(CSV_DIR, f"{code}.csv")
    if csv_need_update(csv_path):
        if code.startswith("sh"):
            symbol = "0" + code[2:]
        else:
            symbol = "1" + code[2:]
        today = datetime.datetime.now().strftime("%Y%m%d")
        url = f"https://quotes.money.163.com/service/chddata.html?code={symbol}&start=20250101&end={today}&fields=TCLOSE"
        df = pd.read_csv(url, encoding='gbk')
        df.to_csv(csv_path, index=False)

    df = pd.read_csv(csv_path, encoding='gbk')
    df = df[df['收盘价'] != 'None']
    df['收盘价'] = df['收盘价'].astype(float)
    ma50 = df['收盘价'].iloc[:50].mean()

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            'REPLACE INTO ma50 (code, value, updated_at) VALUES (?, ?, ?)',
            (code, ma50, today)
        )

    return ma50

def csv_need_update(csv_path):
    if not os.path.exists(csv_path):
        return True
    # 文件修改时间距离现在多久（秒）
    last_modified = os.path.getmtime(csv_path)
    now = time.time()
    # 超过24小时更新一次
    return (now - last_modified) > 24 * 3600