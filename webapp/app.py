# webapp/app.py
import sys
import os

# 把项目根目录（app.py 的上级目录）加入模块搜索路径
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import streamlit as st
import pandas as pd
from utils.filters import run_filter
from utils.stock_list import get_all_stock_info  # ⬅️ 用新的函数
from utils.ma_calculator import init_db

st.set_page_config(page_title="A股筛选器", layout="wide")
st.title("📈 A股低于50日均线筛选器")
st.caption("筛选当前价低于50日均线 7%-8% 的 A 股")

if st.button("开始筛选"):
    with st.spinner("初始化本地数据库..."):
        init_db()

    with st.spinner("获取股票列表与现价中..."):
        price_map = get_all_stock_info()

    with st.spinner("执行多线程筛选中...（预计需 1~2 分钟）"):
        results = run_filter(price_map)

    if not results:
        st.warning("没有符合条件的股票")
    else:
        df = pd.DataFrame(results)
        st.success(f"共筛选出 {len(df)} 支股票")
        st.dataframe(df)

        st.download_button(
            "📥 下载为 Excel",
            df.to_excel(index=False),
            file_name="筛选结果.xlsx"
        )
