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
-- 1. 活动日期：前1天、当天、后1天
event_days AS (

    select event_name
          ,event_date join_date
          ,CDate
          ,pre_1h
          ,pre_2h
          ,pre_4h
          ,post_1h
          ,post_2h
          ,post_4h
    from
    (SELECT
         event_name,
         CDate,
         StartDtm_,
          EndDtm_,
         pre_1h,
         pre_2h,
         pre_4h,
         post_1h,
         post_2h,
         post_4h,


         arrayMap(
            i -> toDate(StartDtm_- interval 1 day) + i,
            range(toUInt32(dateDiff('day', toDate(pre_4h), toDate(post_4h))) + 1)
        ) AS  event_date
    FROM (select
        CDate,
        StartDtm_,
        EndDtm_,
        Title as event_name,

        StartDtm_ - INTERVAL 1 HOUR AS pre_1h,
        StartDtm_ - INTERVAL 2 HOUR AS pre_2h,
        StartDtm_ - INTERVAL 4 HOUR AS pre_4h,

        EndDtm_ + INTERVAL 1 HOUR AS post_1h,
        EndDtm_ + INTERVAL 2 HOUR AS post_2h,
        EndDtm_ + INTERVAL 4 HOUR AS post_4h

    from dim_arena_event_list 
     where event_name = %(date)s    ))
    ARRAY JOIN event_date

),

-- 2. 参加活动的人（去重）
event_attendees AS (
    SELECT DISTINCT
        profile_id,
        toDate(capture_time) AS date,
        hour_window,
        event_name,
        Age_range,
        member_tier,
        profile_type,
        gender
    FROM dwd_arena_capture_detail
    ARRAY JOIN [1, 2, 4] AS hour_window
    WHERE
        event_name IS NOT NULL
        AND multiIf(
            hour_window = 1, time_window BETWEEN -1 AND 1,
            hour_window = 2, time_window BETWEEN -2 AND 2,
            hour_window = 4, time_window BETWEEN -4 AND 4,
            0
        )
        and event_name = %(date)s
),

raw_data AS (
                SELECT
                    profile_id,
                    Age_range,
                    gender,
                    profile_type,
                    member_tier,
                    group,
                    capture_time,
                    event_name,
                    toDecimal64(dateDiff('second', capture_time, coalesce(next_capture_time, capture_time)), 2) AS single_stay_time
                FROM dwd_arena_capture_detail
                where toDate(capture_time) in (select join_date from event_days)
            )



select date
     , event_name
     , Age_range
     , gender
     , profile_type
     , member_tier
     , group
     , hour_window
     , pre_visition_num
     , pre_stay_time
     , post_visition_num
     , post_stay_time
     , now() batch_time
from

    (SELECT
                        e.CDate as date,
                        e.event_name,
                        Age_range,
                        gender,
                        profile_type,
                        member_tier,
                        'all' as group ,
                        -- 将 1, 2, 4 小时的结果聚合到数组中
                         1 AS hour_window,

                         multiIf(max(toDate(e.pre_1h))<e.CDate,count(distinct profile_id),
                                countIf(distinct profile_id, r.capture_time <= e.pre_1h )) pre_visition_num,

                        multiIf(max(toDate(e.pre_1h))<e.CDate,sum(r.single_stay_time),
                                sumIf(r.single_stay_time, r.capture_time <= e.pre_1h )) pre_stay_time,

                        multiIf(max(toDate(e.post_1h))>e.CDate,count(distinct profile_id),
                                countIf(distinct profile_id, r.capture_time >= e.post_1h )) post_visition_num,

                        multiIf(max(toDate(e.post_1h))>e.CDate,sum(r.single_stay_time),
                                sumIf(r.single_stay_time, r.capture_time >= e.post_1h )) post_stay_time

                    FROM event_days AS e
                    -- 使用显式解析出的 join_date 关联
                    INNER JOIN raw_data AS r ON toDate(r.capture_time) = e.join_date
                    where r.profile_id in (select ea.profile_id from event_attendees ea where hour_window=1)
                    GROUP BY e.event_name, e.CDate, Age_range, gender, profile_type, member_tier
       union all

       SELECT
                        e.CDate as date,
                        e.event_name,
                        Age_range,
                        gender,
                        profile_type,
                        member_tier,
                        'all' as group ,
                        -- 将 1, 2, 4 小时的结果聚合到数组中
                         2 AS hour_window,

                         multiIf(max(toDate(e.pre_2h))<e.CDate,count(distinct profile_id),
                                countIf(distinct profile_id, r.capture_time <= e.pre_2h )) pre_visition_num,

                        multiIf(max(toDate(e.pre_2h))<e.CDate,sum(r.single_stay_time),
                                sumIf(r.single_stay_time, r.capture_time <= e.pre_2h )) pre_stay_time,

                        multiIf(max(toDate(e.post_2h))>e.CDate,count(distinct profile_id),
                                countIf(distinct profile_id, r.capture_time >= e.post_2h )) post_visition_num,

                        multiIf(max(toDate(e.post_2h))>e.CDate,sum(r.single_stay_time),
                                sumIf(r.single_stay_time, r.capture_time >= e.post_2h )) post_stay_time
                    FROM event_days AS e
                    -- 使用显式解析出的 join_date 关联
                    INNER JOIN raw_data AS r ON toDate(r.capture_time) = e.join_date
                    where r.profile_id in (select ea.profile_id from event_attendees ea where hour_window=2)
                    GROUP BY e.event_name, e.CDate, Age_range, gender, profile_type, member_tier
  union all

    SELECT
                        e.CDate as date,
                        e.event_name,
                        Age_range,
                        gender,
                        profile_type,
                        member_tier,
                        'all' as group ,
                        -- 将 1, 2, 4 小时的结果聚合到数组中
                         4 AS hour_window,

                         multiIf(max(toDate(e.pre_4h))<e.CDate,count(distinct profile_id),
                                countIf(distinct profile_id, r.capture_time <= e.pre_4h )) pre_visition_num,

                        multiIf(max(toDate(e.pre_4h))<e.CDate,sum(r.single_stay_time),
                                sumIf(r.single_stay_time, r.capture_time <= e.pre_4h )) pre_stay_time,

                        multiIf(max(toDate(e.post_4h))>e.CDate,count(distinct profile_id),
                                countIf(distinct profile_id, r.capture_time >= e.post_4h )) post_visition_num,

                        multiIf(max(toDate(e.post_4h))>e.CDate,sum(r.single_stay_time),
                                sumIf(r.single_stay_time, r.capture_time >= e.post_4h )) post_stay_time
                    FROM event_days AS e
                    -- 使用显式解析出的 join_date 关联
                    INNER JOIN raw_data AS r ON toDate(r.capture_time) = e.join_date
                    where r.profile_id in (select ea.profile_id from event_attendees ea where hour_window=4)
                    GROUP BY e.event_name, e.CDate, Age_range, gender, profile_type, member_tier

     union all

     SELECT
                        e.CDate as date,
                        e.event_name,
                        Age_range,
                        gender,
                        profile_type,
                        member_tier,
                         group ,
                        -- 将 1, 2, 4 小时的结果聚合到数组中
                         1 AS hour_window,

                         multiIf(max(toDate(e.pre_1h))<e.CDate,count(distinct profile_id),
                                countIf(distinct profile_id, r.capture_time <= e.pre_1h )) pre_visition_num,

                        multiIf(max(toDate(e.pre_1h))<e.CDate,sum(r.single_stay_time),
                                sumIf(r.single_stay_time, r.capture_time <= e.pre_1h )) pre_stay_time,

                        multiIf(max(toDate(e.post_1h))>e.CDate,count(distinct profile_id),
                                countIf(distinct profile_id, r.capture_time >= e.post_1h )) post_visition_num,

                        multiIf(max(toDate(e.post_1h))>e.CDate,sum(r.single_stay_time),
                                sumIf(r.single_stay_time, r.capture_time >= e.post_1h )) post_stay_time

                    FROM event_days AS e
                    -- 使用显式解析出的 join_date 关联
                    INNER JOIN raw_data AS r ON toDate(r.capture_time) = e.join_date
                    where r.profile_id in (select ea.profile_id from event_attendees ea where hour_window=1)
                    GROUP BY e.event_name, e.CDate, Age_range, gender, profile_type, member_tier,group
       union all

       SELECT
                        e.CDate as date,
                        e.event_name,
                        Age_range,
                        gender,
                        profile_type,
                        member_tier,
                         group ,
                        -- 将 1, 2, 4 小时的结果聚合到数组中
                         2 AS hour_window,

                         multiIf(max(toDate(e.pre_2h))<e.CDate,count(distinct profile_id),
                                countIf(distinct profile_id, r.capture_time <= e.pre_2h )) pre_visition_num,

                        multiIf(max(toDate(e.pre_2h))<e.CDate,sum(r.single_stay_time),
                                sumIf(r.single_stay_time, r.capture_time <= e.pre_2h )) pre_stay_time,

                        multiIf(max(toDate(e.post_2h))>e.CDate,count(distinct profile_id),
                                countIf(distinct profile_id, r.capture_time >= e.post_2h )) post_visition_num,

                        multiIf(max(toDate(e.post_2h))>e.CDate,sum(r.single_stay_time),
                                sumIf(r.single_stay_time, r.capture_time >= e.post_2h )) post_stay_time
                    FROM event_days AS e
                    -- 使用显式解析出的 join_date 关联
                    INNER JOIN raw_data AS r ON toDate(r.capture_time) = e.join_date
                    where r.profile_id in (select ea.profile_id from event_attendees ea where hour_window=2)
                    GROUP BY e.event_name, e.CDate, Age_range, gender, profile_type, member_tier,group
  union all

    SELECT
                        e.CDate as date,
                        e.event_name,
                        Age_range,
                        gender,
                        profile_type,
                        member_tier,
                        group ,
                        -- 将 1, 2, 4 小时的结果聚合到数组中
                         4 AS hour_window,

                         multiIf(max(toDate(e.pre_4h))<e.CDate,count(distinct profile_id),
                                countIf(distinct profile_id, r.capture_time <= e.pre_4h )) pre_visition_num,

                        multiIf(max(toDate(e.pre_4h))<e.CDate,sum(r.single_stay_time),
                                sumIf(r.single_stay_time, r.capture_time <= e.pre_4h )) pre_stay_time,

                        multiIf(max(toDate(e.post_4h))>e.CDate,count(distinct profile_id),
                                countIf(distinct profile_id, r.capture_time >= e.post_4h )) post_visition_num,

                        multiIf(max(toDate(e.post_4h))>e.CDate,sum(r.single_stay_time),
                                sumIf(r.single_stay_time, r.capture_time >= e.post_4h )) post_stay_time
                    FROM event_days AS e
                    -- 使用显式解析出的 join_date 关联
                    INNER JOIN raw_data AS r ON toDate(r.capture_time) = e.join_date
                    where r.profile_id in (select ea.profile_id from event_attendees ea where hour_window=4)
                    GROUP BY e.event_name, e.CDate, Age_range, gender, profile_type, member_tier  ,group

        ) t1
                            
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

    target_table = ["dws_arena_preAndpost_analysis_v2"]

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






