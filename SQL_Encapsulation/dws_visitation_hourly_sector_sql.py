from datetime import datetime
# from ClickHouseHandler import ClickHouseHandler
from ClickHouseHandler_stream_old import ClickHouseHandler
import pandas as pd

class dws_visitation_hourly_sector:

    def __init__(self, host=["localhost", "localhost"], port=[9000, 9000], user=["default", "default"],
                 password=["", ""], database=["default", "default"], target_table=None, date_list=[]):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.target_table = target_table
        self.date_list = date_list

    def main(self):
        # date=datetime.strptime("2025-08-25", "%Y-%m-%d").date()

        # date_list = ["2025-08-25", "2025-08-26", "2025-08-27"]
        # target_table = "dws_visitation_demographics"

        delete_sql = f"alter table  {self.target_table} delete where  date_casino=%(date)s"

        source_sql = f"""
                     
                        SELECT date_casino 
                  ,date_hour 
                  ,region_id
                  ,region_name 
                  ,sector 
                  ,sum(visitor_num) visitor_num
                  ,now() batch_time 
            from  dws_visitation_hourly t1
            where date_casino= %(date)s
            and region_id <> 'all'
            group by date_casino ,date_hour ,region_id,region_name ,sector 
            union all
            
            select   
                    toDate(date_casino_hour,'Asia/Shanghai') AS date_casino
                    , date_hour
                    , 'all' region_id
                    , 'all' region_name
                    , case when  region_type = '4' then 'Gaming'  else sector end sector 
                    , count(distinct profile_id) visitor_num
                    , now() batch_time
            from dws_profileid_all_index t1
            where date_casino = %(date)s
            group by  date_casino,date_hour,sector
            
    

        """

        ch = ClickHouseHandler(host=self.host, port=self.port, user=self.user, password=self.password,
                               database=self.database, prefix=self.target_table)

        for date in self.date_list:
            ch.delete_partition(delete_sql, self.target_table, {"date": date})
            ch._insert_into_select(source_sql, self.target_table, {"date": date})
            # ch.stream_query_insert(source_sql, target_table,{"date":date},1000)


if __name__ == "__main__":
    host = ['localhost', 'localhost']
    port = [9000, 9000]
    user = ['default', 'default']
    password = ['ck_test', 'ck_test']
    database = ['Facial', 'Facial']
    date_list=pd.date_range("2025-10-01", "2025-10-31").strftime("%Y-%m-%d").tolist()

    target_table = ["dwd_user_capture_detail"
        , "dws_profileid_group"
        , "dws_profileid_staytime"
        , "dws_profileid_NewOrReturn_visitor"
        , "dws_profileid_all_index"
        , "dws_visitation_daily_casino"
        , "dws_visitation_daily"
        , "dws_visitation_group_casino"
        , "dws_visitation_hourly_casino"
        , "dws_visitation_hourly_sector"
                    ]
    # date_list = ["2025-08-25", "2025-08-26", "2025-08-27"]

    vd = dws_visitation_hourly_sector(host=host, port=port, user=user, password=password, database=database, target_table=target_table[9], date_list=date_list)
    vd.main()





