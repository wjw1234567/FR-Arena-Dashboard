from datetime import datetime
# from ClickHouseHandler import ClickHouseHandler
from ClickHouseHandler_stream_old import ClickHouseHandler
import pandas as pd

class dws_visitor_path_track_heatmap_daily:

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


               with tab_profile as (
                   select toDate(date_hour) date
                   ,toDate(date_casino_hour) date_casino
             , profile_id
             , region_type
             , region_id
             , region_name
             , gender
             , Age_range
             , profile_type
             , member_tier
             , sector
             , max(stay_time) stay_time_p
             , case when region_type='3'and max(stay_time) < 15*60 then 1 else 0 end is_less_15min_region
             , case when region_type='4'and max(stay_time) < 15*60 then 1 else 0 end is_less_15min_casino
             , is_return_zone
             , is_return_casino
       from  dws_profileid_all_index
       where toDate(date_casino_hour)=%(date)s
       group by date,date_casino,profile_id, region_type, region_id, region_name, gender, Age_range, profile_type, member_tier, is_return_zone, is_return_casino,sector

            )


               select
                  date_casino
               , region_type
               , case when region_type='4' then 'CASINO'
                      when region_name like 'BW%%' then 'BW'
                      else region_name end `region_name`
               , gender
               , Age_range
               , profile_type
               , member_tier
               , case when  region_type = '4' then 'Gaming'  else sector end  sector
               , count(distinct profile_id) visitor_num
            
               , now() batch_time

       from  tab_profile t1
       group by date_casino, region_type, `region_name`, gender, Age_range, profile_type, member_tier,sector


           """




        ch = ClickHouseHandler(host=self.host, port=self.port, user=self.user, password=self.password,
                               database=self.database, prefix=self.target_table)

        for date in self.date_list:
            ch.delete_partition(delete_sql, self.target_table, {"date": date})
            ch._insert_into_select(source_sql, self.target_table, {"date": date})


if __name__ == "__main__":
    host = ['localhost', 'localhost']
    port = [9000, 9000]
    user = ['default', 'default']
    password = ['ck_test', 'ck_test']
    database = ['Facial', 'Facial']
    target_table = ["dwd_user_capture_detail", "dws_profileid_group", "dws_profileid_NewOrReturn_visitor",
                    "dws_profileid_staytime", "dws_visitation_analytics_and_casino_entrances",
                    "dws_visitation_hourly","dws_visitor_path_track_heatmap_daily"]
    # date_list = ["2025-08-25", "2025-08-26", "2025-08-27"]

    date_list = pd.date_range("2025-08-01", "2025-08-31").strftime("%Y-%m-%d").tolist()

    vd = dws_visitor_path_track_heatmap_daily(host=host, port=port, user=user, password=password, database=database,
                                     target_table=target_table[6], date_list=date_list)
    vd.main()





