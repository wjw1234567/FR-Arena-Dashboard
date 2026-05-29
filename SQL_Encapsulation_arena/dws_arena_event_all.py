from datetime import datetime
# from ClickHouseHandler import ClickHouseHandler
from ClickHouseHandler_stream import ClickHouseHandler
import pandas as pd
from clickhouse_driver import Client



class dws_arena_event_all:

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
                 
         select t1.date date
     , t1.hour_window hour_window
     , t1.event_name event_name
     , t1.Age_range Age_range
     , t1.member_tier member_tier
     , t1.profile_type profile_type
     , t1.gender gender
     , t1.group as group
     , t1.visition_num visition_num
     , t1.casino_converted_visition_num casino_converted_visition_num
     , t2.post_stay_time post_stay_time
     , t2.post_visition_num post_visition_num
     , t2.pre_stay_time pre_stay_time
     , t2.pre_visition_num pre_visition_num
     , now() batch_time
from dws_arena_casino_conversion_analysis t1
join dws_arena_preAndpost_analysis t2
on   t1.date = t2.date
     and t1.hour_window=t2.hour_window
     and t1.event_name=t2.event_name
     and t1.Age_range= t2.Age_range
     and t1.group=t2.group
     and ifNull(t1.member_tier,'')=ifNull(t2.member_tier,'')
     and t1.profile_type= t2.profile_type
     and t1.gender= t2.gender
     where t1.event_name=%(date)s

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

    target_table = ["dws_arena_event_all"]

    # date_list = pd.date_range("2025-08-01", "2025-08-01").strftime("%Y-%m-%d").tolist()

    get_event_sql = f"""

        select distinct Title event_name from dim_arena_event_list
             where CDate <= toDate(now())  


    """

    ch = ClickHouseHandler(host=host, port=port, user=user,password=password, database=database)
    date_list=ch._get_date_list(get_event_sql)


    cd = dws_arena_event_all(host=host, port=port, user=user, password=password, database=database,
                              target_table=target_table[0],date_list=date_list)
    cd.main()






