from datetime import datetime
# from ClickHouseHandler import ClickHouseHandler
from ClickHouseHandler_stream import ClickHouseHandler
import pandas as pd
from clickhouse_driver import Client



class dwd_arena_capture_detail:

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


        delete_sql = f" alter table  {self.target_table} delete where toDate(capture_time) = %(date)s"

        source_sql = f"""
                  select
    t1.profile_id profile_id,
    t1.age age,
    t1.Age_range,
    t1.gender gender,
    t1.profile_type profile_type,
    t1.member_tier member_tier,
    toDateTime(t1.capture_time,'UTC') capture_time,
    toDateTime(t1.next_capture_time,'UTC') next_capture_time,
    t2.group `group`,
    t2.business_sector business_sector,
    t2.camera_id camera_id,

    case when t1.capture_time between del.event_start and del.event_end and t2.business_sector='Arena' then del.Title end event_name,
    case when t1.capture_time between del.event_start and del.event_end and t2.business_sector='Arena' then del.CDate end CDate,
    
    case when t1.capture_time between del.event_start and del.event_end and t2.business_sector='Arena' then del.StartDtm_
        else NULL end StartDtm_event,

    case when t1.capture_time between del.event_start and del.event_end and t2.business_sector='Arena' then del.EndDtm_
        else NULL end EndDtm_event,

--     if(toYear(del.StartDtm,'Asia/Shanghai') <= 1970, NULL, toDateTime(del.StartDtm,'Asia/Shanghai')) as `StartDtm`,
--     if(toYear(del.EndDtm,'Asia/Shanghai') <= 1970, NULL, toDateTime(del.EndDtm,'Asia/Shanghai')) as `EndDtm`,

     case when event_name is not null and  dateDiff('minute', del.StartDtm_ , t1.capture_time) <0 and dateDiff('minute', del.StartDtm_ , t1.capture_time) >-60 then -1
         when event_name is not null and dateDiff('minute', del.StartDtm_ , t1.capture_time) <=-60 and dateDiff('minute', del.StartDtm_ , t1.capture_time) >-120 then -2
         when event_name is not null and dateDiff('minute', del.StartDtm_ , t1.capture_time) <=-120 and dateDiff('minute', del.StartDtm_ , t1.capture_time) >=-240 then -4
         when event_name is not null and  (dateDiff('minute', del.StartDtm_ , t1.capture_time) = 0 or dateDiff('minute', del.EndDtm_, t1.capture_time)=0
                                           or t1.capture_time between del.StartDtm_ and del.EndDtm_) then 0
         when event_name is not null and dateDiff('minute', del.EndDtm_, t1.capture_time) >0 and dateDiff('minute', del.EndDtm_ , t1.capture_time) <60 then 1
         when event_name is not null and dateDiff('minute', del.EndDtm_, t1.capture_time) >=60 and dateDiff('minute', del.EndDtm_ , t1.capture_time) <120 then 2
         when event_name is not null and dateDiff('minute', del.EndDtm_, t1.capture_time) >=120 and dateDiff('minute', del.EndDtm_ , t1.capture_time) <=240 then 4
    else null end time_window,

    now() batch_time
from
    (select
         profile_id
        ,age
        ,case when age between 0 and 20 then '0-20'
                                     when age between '21' and '39' then '21-39'
                                     when age between '40' and '65' then '40-65'
                                     when age >'65' then  '65+'
                               end   Age_range
        ,gender
        ,profile_type
        ,member_tier
        ,capture_time
        ,camera_id
        ,leadInFrame(toNullable(capture_time),1,toNullable(null)) over(partition by (toDate(capture_time),profile_id)  ORDER BY profile_id,capture_time RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) next_capture_time

         from dwd_arena_capture_original
        where toDate(capture_time) =  %(date)s


         ) t1

join dim_arena_camera t2 on toString(t1.camera_id) = toString(t2.camera_id)
left join (

    select
        Title,
        Venue,
        StartDtm,
        EndDtm,
        CDate,
        StartDtm_,
        EndDtm_,
        addHours(StartDtm_, -4) as event_start,
        addHours(EndDtm_, 4) as event_end,
        Period_Flag
    from dim_arena_event_list
    ) del on toDate(t1.capture_time,'UTC')=toDate(del.StartDtm_,'UTC')

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

    target_table = ["dwd_arena_capture_detail"]

    # date_list = pd.date_range("2025-08-01", "2025-08-01").strftime("%Y-%m-%d").tolist()

    get_datelist_sql = f"""

        select distinct CDate from
            (select CDate from dim_arena_event_list
            union all
            select CDate - interval 1 Day from dim_arena_event_list
            union all
            select CDate + interval 1 Day from dim_arena_event_list) CD
            where CDate <= toDate(now())  


    """

    ch = ClickHouseHandler(host=host, port=port, user=user,password=password, database=database)
    date_list=ch._get_date_list(get_datelist_sql)


    cd = dwd_arena_capture_detail(host=host, port=port, user=user, password=password, database=database,
                              target_table=target_table[0],date_list=date_list)
    cd.main()






