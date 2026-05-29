from datetime import datetime
# from ClickHouseHandler import ClickHouseHandler
from ClickHouseHandler_stream_old import ClickHouseHandler
import pandas as pd

class dws_visitation_group_casino:

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

        delete_sql = f"alter table  {self.target_table} delete where  date=%(date)s"

        source_sql = f"""
         
               select
                  date
                , date_casino
                ,region_id
                ,region_name
                , case when  group_size =1 then '1'
                       when  group_size =2 then '2'
                       when  group_size =3 then '3'
                       when  group_size =4 then '4'
                       when  group_size =5 then '5'
                  else '5+' end group_type
                , sum(group_size) group_visitor_num
                ,now() batch_time
                from
            (
            select toDate(date_hour) date
                   ,toDate(date_casino_hour) date_casino
                   ,group_id
                   ,region_id
                   ,region_name
                   ,count(distinct profile_id) group_size
            from dws_profileid_group
            where date_casino = %(date)s
            group by date,date_casino,group_id,region_id,region_name
                ) aa
                group by date, date_casino,region_id,region_name,group_type
            union all
            select
                  date
                , date_casino
                ,'all' region_id
                ,'all' region_name
                , case when  group_size =1 then '1'
                       when  group_size =2 then '2'
                       when  group_size =3 then '3'
                       when  group_size =4 then '4'
                       when  group_size =5 then '5'
                  else '5+' end group_type
                , sum(group_size) group_visitor_num
                ,now() batch_time
                from
            (
            select toDate(date_hour) date
                   ,toDate(date_casino_hour) date_casino
                   ,group_id
                   ,count(distinct profile_id) group_size
            from  dws_profileid_group
            where date_casino = %(date)s
            group by date,date_casino,group_id,region_id,region_name ) aa
                group by date, date_casino,group_type

        """

        ch = ClickHouseHandler(host=self.host, port=self.port, user=self.user, password=self.password,
                               database=self.database, prefix=self.target_table)

        for date in self.date_list:
            ch.delete_partition(delete_sql, self.target_table, {"date": date})
            ch.stream_query_insert(source_sql, self.target_table, {"date": date})
            # ch.stream_query_insert(source_sql, target_table,{"date":date},1000)


if __name__ == "__main__":
    host = ['localhost', 'localhost']
    port = [9000, 9000]
    user = ['default', 'default']
    password = ['ck_test', 'ck_test']
    database = ['Facial', 'Facial']
    target_table = ["dwd_user_capture_detail", "dws_profileid_group", "dws_profileid_NewOrReturn_visitor",
                    "dws_profileid_staytime", "dws_visitation_analytics_and_casino_entrances",
                    "dws_visitation_hourly","dws_visitation_group_casino"]
    date_list = pd.date_range("2025-10-01", "2025-10-31").strftime("%Y-%m-%d").tolist()

    vd = dws_visitation_group_casino(host=host, port=port, user=user, password=password, database=database,
                                     target_table=target_table[6], date_list=date_list)
    vd.main()





