# webapp/app.py
import sys
import os

# 把项目根目录（app.py 的上级目录）加入模块搜索路径
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import streamlit as st
import pandas as pd
from utils.ak import get_dip_stock

st.set_page_config(page_title="A股筛选器", layout="wide")
st.title("筛选器")
# st.caption("筛选当前价低于50日均线 7% 的 A 股")

if st.button("开始筛选"):
    '''
            result数据结构
            {
            'code': stock[0],
            'name': stock[1],
            'ma50': stock[2],
            'price': stock[3],
            'deviation': stock[4]
            }
            '''
    with st.spinner("执行多线程筛选中...（预计需 1~2 分钟）"):

        results = get_dip_stock()

    if not results:
        st.warning("没有符合条件的股票")
    else:
        df = pd.DataFrame(results)
        st.success(f"共筛选出 {len(df)} 支股票")
        st.dataframe(df)

        # st.download_button(
        #     "📥 下载为 Excel",
        #     df.to_excel(index=False),
        #     file_name="筛选结果.xlsx"
        # )
