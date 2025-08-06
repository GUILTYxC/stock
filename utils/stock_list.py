# utils/stock_list.py
from datetime import datetime
import sqlite3
import time

import requests

DB_PATH = '../ma_cache.db'

def get_all_stock_info(max_pages=11):
    result = {}
    page = 1
    page_size = 500

    while page <= max_pages:
        url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
        params = {
            "sortColumns": "HOLD_NOTICE_DATE,SECURITY_CODE",
            "sortTypes": "-1,-1",
            "pageSize": page_size,
            "pageNumber": page,
            "reportName": "RPT_HOLDERNUMLATEST",
            "columns": "SECURITY_CODE,SECURITY_NAME_ABBR",
            "quoteColumns": "f2,f3",
            "quoteType": 0,
            "source": "WEB",
            "client": "WEB"
        }

        try:
            resp = requests.get(url, params=params, timeout=10)
            data = resp.json()
            rows = data.get("result", {}).get("data", [])
            if not rows:
                break

            for row in rows:

                code = row.get("SECURITY_CODE")
                name = row.get("SECURITY_NAME_ABBR")
                price = row.get("f2")  # 当前价

                if price is None:
                    continue
                prefix = "sh" if code.startswith("6") else "sz"
                full_code = prefix + code
                result[code] = {
                    "name": name,
                    "price": float(price),
                    "full_code": full_code,
                }

            print(f"[+] 已处理第 {page} 页，共获取 {len(result)} 支股票")
            page += 1
            time.sleep(0.2)

        except Exception as e:
            print(f"[!] 第 {page} 页失败: {e}")
            break

    return result


def update_today_prices_from_info(stock_info: dict):
    today = datetime.now().strftime('%Y-%m-%d')
    data = [(code, today, info["price"]) for code, info in stock_info.items()]
    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany(
            'REPLACE INTO stock_prices (code, date, close) VALUES (?, ?, ?)',
            data
        )
        conn.commit()
