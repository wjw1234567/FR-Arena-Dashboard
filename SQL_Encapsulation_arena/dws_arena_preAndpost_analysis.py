from datetime import datetime
# from ClickHouseHandler import ClickHouseHandler
from ClickHouseHandler_stream import ClickHouseHandler
import pandas as pd
from clickhouse_driver import Client



class dws_arena_preAndpost_analysis:

    def __init__(self, host=["localhost", "localhost"], port=[9000, 9000], user=["default", "default"],
                 password=["", ""], database=["default", "default"], target_table=None,date_list=[]):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.target_table = target_table
        self.date_list = date_list

    def main(self):
        # date=datetime.strptime("2025-08-25", "%Y-%m-%d").date()

        # date_list=["2025-08-25","2025-08-26","2025-08-27"]
        # target_table = "dwd_user_capture_detail"


        delete_sql = f" alter table  {self.target_table} delete where `event_name` = %(date)s"

        source_sql = f"""
                  
WITH
-- 1. 活动日期：前1天、当天、后1天 (原逻辑保持不变)
event_days AS (
    SELECT event_name,
           event_date AS join_date,
           CDate, pre_1h, pre_2h, pre_4h, post_1h, post_2h, post_4h,StartDtm_,EndDtm_
    FROM (
        SELECT event_name, CDate, StartDtm_, EndDtm_,
               pre_1h, pre_2h, pre_4h, post_1h, post_2h, post_4h,
               arrayMap(
                   i -> toDate(pre_4h ) + i,
                   range(toUInt32(dateDiff('day', toDate(pre_4h), toDate(post_4h))) + 1)
               ) AS event_date
        FROM (
            SELECT CDate, StartDtm_, EndDtm_,
                   Title AS event_name,
                   StartDtm_ - INTERVAL 1 HOUR AS pre_1h,
                   StartDtm_ - INTERVAL 2 HOUR AS pre_2h,
                   StartDtm_ - INTERVAL 4 HOUR AS pre_4h,
                   EndDtm_ + INTERVAL 1 HOUR AS post_1h,
                   EndDtm_ + INTERVAL 2 HOUR AS post_2h,
                   EndDtm_ + INTERVAL 4 HOUR AS post_4h
            FROM dim_arena_event_list
            WHERE event_name = %(date)s 
        )
    ) ARRAY JOIN event_date
),

-- 2. 参加活动的人：使用 groupUniqArray 将同一个用户符合的多个时间窗口聚合为数组
event_attendees AS (
    SELECT profile_id,
           groupUniqArray(hour_window) AS valid_windows
    FROM (
        SELECT DISTINCT profile_id, hour_window
        FROM dwd_arena_capture_detail
        ARRAY JOIN [1, 2, 4] AS hour_window
        WHERE event_name = %(date)s 
          AND (
              (hour_window = 1 AND time_window BETWEEN -1 AND 1) OR
              (hour_window = 2 AND time_window BETWEEN -2 AND 2) OR
              (hour_window = 4 AND time_window BETWEEN -4 AND 4)
          )
    )
    GROUP BY profile_id
),

-- 3. 基础明细数据：将 IN 替换为 INNER JOIN，避免低效过滤，并同时关联 valid_windows 数组
raw_data AS (
    SELECT r.profile_id, r.Age_range, r.gender, r.profile_type,
           r.member_tier, r.group, r.capture_time, r.event_name,
           toDecimal64(dateDiff('second', r.capture_time, coalesce(r.next_capture_time, r.capture_time)), 2) AS single_stay_time,
           ea.valid_windows
    FROM dwd_arena_capture_detail AS r
    -- 核心优化: INNER JOIN 替代 IN (SELECT ...)
    INNER JOIN event_attendees AS ea ON r.profile_id = ea.profile_id
    WHERE toDate(r.capture_time) IN (SELECT join_date FROM event_days)
)

-- 4. 主干查询：利用 ARRAY JOIN 和条件表达式，彻底干掉 6个 UNION ALL
SELECT date, event_name, Age_range, gender, profile_type, member_tier,
       final_group AS `group`,
       hour_window,

               countIf(DISTINCT profile_id, capture_time between pre_th and StartDtm_ ) AS pre_visition_num,

               sumIf(single_stay_time, capture_time between pre_th and StartDtm_) AS pre_stay_time,

               countIf(DISTINCT profile_id, capture_time between EndDtm_ and  post_th) AS post_visition_num,

               sumIf(single_stay_time, capture_time between EndDtm_ and post_th) AS post_stay_time,

       now() AS batch_time
FROM (
    SELECT e.CDate AS date, e.event_name, r.Age_range, r.gender, r.profile_type, r.member_tier,
           final_group, hour_window, r.profile_id, r.capture_time, r.single_stay_time,
           e.StartDtm_ StartDtm_,e.EndDtm_ EndDtm_,
           -- 动态映射前置/后置时间阈值
           multiIf(hour_window = 1, e.pre_1h, hour_window = 2, e.pre_2h, e.pre_4h) AS pre_th,
           multiIf(hour_window = 1, e.post_1h, hour_window = 2, e.post_2h, e.post_4h) AS post_th
    FROM event_days AS e
    INNER JOIN raw_data AS r ON toDate(r.capture_time) = e.join_date
    -- 核心优化: 在内存中以行复制取代 UNION ALL。将维度和窗口直接展开计算
    ARRAY JOIN r.valid_windows AS hour_window
    ARRAY JOIN  [r.group, 'all'] AS final_group
) AS base_flat
GROUP BY date, event_name, Age_range, gender, profile_type, member_tier, final_group, hour_window

        
        
                            
        """

        ch = ClickHouseHandler(host=self.host, port=self.port, user=self.user, password=self.password,
                               database=self.database, prefix=self.target_table)


        for date in self.date_list:
            ch.delete_partition(delete_sql, self.target_table, {"date": date})
            # ch.stream_query_insert(source_sql, self.target_table,{"date":date},batch_size=10000)
            ch._insert_into_select(source_sql, self.target_table, {"date": date})


if __name__ == "__main__":
    # 读的IP和写的IP
    host = ['localhost', 'localhost']
    port = [9000, 9000]
    user = ['default', 'default']
    password = ['ck_test', 'ck_test']
    database = ['Facial', 'Facial']

    target_table = ["dws_arena_preAndpost_analysis"]

    # date_list = pd.date_range("2025-08-01", "2025-08-01").strftime("%Y-%m-%d").tolist()

    get_event_sql = f"""

        select distinct Title event_name from dim_arena_event_list
             where CDate <= toDate(now())  


    """

    ch = ClickHouseHandler(host=host, port=port, user=user,password=password, database=database)
    date_list=ch._get_date_list(get_event_sql)


    cd = dws_arena_preAndpost_analysis(host=host, port=port, user=user, password=password, database=database,
                              target_table=target_table[0],date_list=date_list)
    cd.main()






