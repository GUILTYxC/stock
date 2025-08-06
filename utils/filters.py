# filters.py
from concurrent.futures import ThreadPoolExecutor
from utils.ma_calculator import get_ma50


def check_one(code, info):
    ma50 = get_ma50(code)
    current_price = info['price']
    name = info['name']
    if current_price and ma50:
        ratio = current_price / ma50
        if 0.92 <= ratio <= 0.93:
            return {
                'code': code,
                'name': name,
                'current': round(current_price, 2),
                'ma50': round(ma50, 2),
                'deviation%': round((ratio - 1) * 100, 2)
            }
    return None


def run_filter(price_map: dict, max_workers=10):
    result = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(check_one, code, info)
            for code, info in price_map.items()
        ]
        for f in futures:
            r = f.result()
            if r:
                result.append(r)
    return result
