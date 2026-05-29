from datetime import datetime
# from ClickHouseHandler import ClickHouseHandler
from ClickHouseHandler_stream import ClickHouseHandler
import pandas as pd
from clickhouse_driver import Client



class dws_arena_casino_conversion_analysis:

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
        
            select event_name,event_date from
            (SELECT
                 event_name,
                 arrayMap(
                    i -> toDate(StartDtm_- interval 1 day) + i,
                    range(toUInt32(dateDiff('day', toDate(StartDtm_ - interval 1 day), toDate(EndDtm_+ interval 1 day))) + 1)
                ) AS  event_date
            FROM (select
                CDate,
                StartDtm_,
                EndDtm_,
                Title as event_name
            from dim_arena_event_list  ))
            ARRAY JOIN event_date
            where event_name = %(date)s
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
                gender,
                group
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

        -- 3. 赌场入口的人（去重）
        casino_visitors AS (
            SELECT DISTINCT
                profile_id,
                toDate(capture_time) AS date_casino
            FROM dwd_arena_capture_detail
            WHERE business_sector = 'Casino Entrance'
            and date_casino in (select distinct event_date from event_days)
        )

        ,final_attendees as (
                SELECT
                e.date date,
                e.hour_window hour_window,
                e.event_name event_name,
                e.Age_range Age_range,
                e.member_tier member_tier,
                e.profile_type profile_type,
                e.gender gender,
                e.profile_id profile_id,
                e.group as `group`,
                if(c.profile_id !=0 , 1, null) AS has_visited_casino
            FROM event_attendees e
            LEFT JOIN event_days d ON e.event_name = d.event_name and e.date=d.event_date
            LEFT JOIN casino_visitors c ON e.profile_id = c.profile_id and c.date_casino=d.event_date
        
            )


select
   date,
   hour_window,
   event_name,
   Age_range,
   member_tier,
   profile_type,
   gender,
   group,
   visition_num,
   casino_converted_visition_num,
   now() batch_time
  from 
(SELECT
    date,
    hour_window,
    event_name,
    Age_range,
    member_tier,
    profile_type,
    gender,
    'all' as group,
    COUNT(DISTINCT profile_id) AS visition_num,
    count(distinct if(has_visited_casino=1,profile_id,null))   casino_converted_visition_num
FROM final_attendees
GROUP BY date, hour_window, event_name, Age_range, member_tier, profile_type, gender
union all
SELECT
    date,
    hour_window,
    event_name,
    Age_range,
    member_tier,
    profile_type,
    gender,
    group,
    COUNT(DISTINCT profile_id) AS visition_num,
    count(distinct if(has_visited_casino=1,profile_id,null))   casino_converted_visition_num
FROM final_attendees
GROUP BY date,group, hour_window, event_name, Age_range, member_tier, profile_type, gender) t1
                 
                 

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

    target_table = ["dws_arena_casino_conversion_analysis"]

    # date_list = pd.date_range("2025-08-01", "2025-08-01").strftime("%Y-%m-%d").tolist()

    get_event_sql = f"""

        select distinct Title event_name from dim_arena_event_list
             where CDate <= toDate(now())  


    """

    ch = ClickHouseHandler(host=host, port=port, user=user,password=password, database=database)
    date_list=ch._get_date_list(get_event_sql)


    cd = dws_arena_casino_conversion_analysis(host=host, port=port, user=user, password=password, database=database,
                              target_table=target_table[0],date_list=date_list)
    cd.main()






