from datetime import datetime
# from ClickHouseHandler import ClickHouseHandler
from ClickHouseHandler_stream_old import ClickHouseHandler
import pandas as pd

class dws_visitation_hourly_casino:

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

             select
                   toDate( date_casino_hour) AS date_casino
                , date_hour
                , region_type
                , region_id
                , region_name
                , gender
                , Age_range
                , profile_type
                , member_tier
                , count(distinct profile_id) visitor_num
                , now() batch_time
        from dws_profileid_all_index t1
        where date_casino = %(date)s and  region_type = '4'
        group by  date_casino,date_hour, region_type, region_id, region_name, gender, Age_range, profile_type,member_tier
        union all
        select

                  toDate( date_casino_hour) AS date_casino
                , date_hour
                , region_type
                , 'all' region_id
                , 'all' region_name
                , gender
                , Age_range
                , profile_type
                , member_tier
                , count(distinct profile_id) visitor_num
                , now() batch_time
        from dws_profileid_all_index t1
        where date_casino = %(date)s  and  region_type = '4'
        group by  date_casino,date_hour,region_type, region_type, region_id, region_name, gender, Age_range, profile_type,member_tier


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
    target_table = ["dwd_user_capture_detail", "dws_profileid_group", "dws_profileid_NewOrReturn_visitor",
                    "dws_profileid_staytime", "dws_visitation_analytics_and_casino_entrances",
                    "dws_visitation_hourly_casino"]
    date_list = pd.date_range("2025-10-01", "2025-10-31").strftime("%Y-%m-%d").tolist()
    vd = dws_visitation_hourly_casino(host=host, port=port, user=user, password=password, database=database,
                                     target_table=target_table[5], date_list=date_list)
    vd.main()





