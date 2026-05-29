import pandas as pd
import numpy as np
from clickhouse_driver import Client
from datetime import datetime
from joblib import Parallel, delayed
from collections import deque


# ==========================
# 1. ClickHouse 连接
# ==========================
client = Client(
    host='localhost',
    port=9000,
    user='default',
    password='ck_test',
    database='Facial',
    send_receive_timeout=60
)




# ==========================
# 3. 主流程：整天拉取 → groupby 并行 → 写回
# ==========================
def main():
    # 拿到所有 distinct date
    # date_list = client.execute("SELECT DISTINCT toDate(capture_time) as d FROM dwd_user_capture_heatmap ORDER BY d")

    # 获取全部日期
    date_list = ['2025-08-25']


    target_bins = [-60, -45, -30, -15, -10, -5, 0, 5, 10, 15, 30, 45, 60]

    for date in date_list:
        query = f"""
             SELECT profile_id, profile_type, member_tier, age, gender,
               camera_id, region_id, region_name, capture_time
        FROM dwd_user_capture_original
        WHERE toDate(capture_time) = '{date}' and profile_id=1
        ORDER BY profile_id, capture_time
        """
        rows, cols = client.execute(query, with_column_types=True)
        columns = [c[0] for c in cols]
        df = pd.DataFrame(rows, columns=columns)

        df = df.sort_values(["profile_id", "capture_time"]).copy()

        result_list = []

        for pid, g in df.groupby("profile_id"):
            g = g.sort_values("capture_time")
            g["next_capture_time"]=g["capture_time"].shift(-1)
            g["next_region_name"]=g["region_name"].shift(-1)

            # 去掉连续相同的区域，只保留第一次进入
            mask = g["region_name"] != g["next_region_name"]
            g_unique = g[mask].copy()
            # print(g[['profile_id','region_name','capture_time']].head(20))
            # print(g_unique[['profile_id','region_name','capture_time','next_capture_time']].head(20))

            # pivot=g_unique.pivot_table(index="region_name", columns="profile_id", values="capture_time")
            # print(pivot)

        df = pd.DataFrame({
            "group": list("AABBAAB"),
            "value": [10, 20, 30, 40, 50, 60, 70]
        })

        # 每组减去均值（中心化）transform函数用法
        df["centered"] = df["value"] - df.groupby("group")["value"].transform("mean")

        # print(df.head(20))
        # range=当前分组值的最大值减去最小值
        df["range"] = df.groupby("group")["value"].transform(lambda x: x.max() - x.min())
        # print(df.head(20))

        da = pd.DataFrame({'Q1': [80, 90], 'Q2': [70, 85]})

        # 动态计算总成绩和是否达标
        da = da.assign(
            总成绩=lambda d: d.Q1 + d.Q2,
            达标=lambda d: (d.总成绩 > 150).astype(int)
        )
        # print(da)

        dt1 = pd.DataFrame({'datetime_str': ['2023-10-01 08:30', '2023-10-01 09:45','2023-10-01 09:58','2023-10-01 10:38']})
        dt1['datetime'] = pd.to_datetime(dt1['datetime_str'])
        # 对整列时间数据进行差分计算
        dt1['time_diff'] = dt1['datetime'].diff().dt.total_seconds()

        # 对datetime行进行加减时间
        dt1['new_time'] = dt1['datetime'] + pd.Timedelta(hours=1, minutes=30)

        # np.isin

        print(dt1)




if __name__ == "__main__":
    main()