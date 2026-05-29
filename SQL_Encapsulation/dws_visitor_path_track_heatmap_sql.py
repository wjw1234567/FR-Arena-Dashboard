from datetime  import datetime
# from ClickHouseHandler import ClickHouseHandler

from ClickHouseHandler_stream_old import ClickHouseHandler
from clickhouse_driver import Client
import pandas as pd

# if __name__ == "__main__":

class dws_visitor_path_track_heatmap:

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
        # target_table = "dws_visitor_path_track_heatmap"

        delete_sql = f"alter table  {self.target_table} delete where  date_casino=%(date)s"

        source_sql = f"""


         SELECT
            toDate(v1.capture_time-21600) date_casino,
            formatDateTime(v1.capture_time,'%%H:00') date_hour,
            count(distinct v1.profile_id) visitor_num,
            v1.region_name region_name,
            v1.region_name_heatmap region_name_heatmap,
            toStartOfHour(v1.capture_time) z0_time,
             /* 2) 计算动态 offset，并分桶 判定*/
            case when
                   v1.capture_time != v2.capture_time and
                     dateDiff('minute', v1.capture_time, v2.capture_time) between 0 and 15
                then  (intDiv(dateDiff('minute', v1.capture_time, v2.capture_time), 5)+1) * 5
    
                 when dateDiff('minute', v1.capture_time, v2.capture_time) >= -15
                          and  dateDiff('minute', v1.capture_time, v2.capture_time) < 0
                then  (intDiv(dateDiff('minute', v1.capture_time, v2.capture_time), 5)-1) * 5
    
                 when dateDiff('minute', v1.capture_time, v2.capture_time) between 16 and 60
                 then  (intDiv(dateDiff('minute', v1.capture_time, v2.capture_time), 15)+1) * 15
                 when dateDiff('minute', v1.capture_time, v2.capture_time) between -60 and -16
                 then  (intDiv(dateDiff('minute', v1.capture_time, v2.capture_time), 15)-1) * 15
                when dateDiff('minute', v1.capture_time, v2.capture_time) = 0 then 0
                end  AS off_bin
    
            /*
             找出每个区域作为基准点最近时间差的区域
             */
 

            ,case when off_bin = 0 then v1.region_name else argMin(v2.region_name, off_bin) end AS area_at_off
            ,case when off_bin = 0 then v1.region_name_heatmap else argMin(v2.region_name_heatmap, off_bin) end AS area_at_off_heatmap
    
            , v1.member_tier member_tier
            , v1.profile_type profile_type
            , case when v1.age between 0 and 20 then '0-20'
                 when v1.age between 21 and 39 then '21-39'
                when v1.age between 40 and 65 then '40-65'
                when v1.age>65 then '65+'
              end age_range
             , v1.gender gender

             
             , now() batch_time
    
                FROM (select profile_id,capture_time,member_tier,age,gender,profile_type
                     ,case when property='BW' then 'BW' 
                           when region_type='4' then 'CASINO' 
                     else region_name end region_name

                     ,case when property='BW' then 'BW' 
                           when region_type='4' then 'CASINO' 
                     else region_name_heatmap end region_name_heatmap
                     
                      from  dwd_user_capture_detail
                      where toDate(capture_time-21600) =  %(date)s
                    ) v1
                JOIN  (select profile_id,capture_time,member_tier,age,gender
                     ,case when property='BW' then 'BW' 
                           when region_type='4' then 'CASINO' 
                     else region_name end region_name

                     ,case when property='BW' then 'BW' 
                           when region_type='4' then 'CASINO' 
                     else region_name_heatmap end region_name_heatmap
                     
                      from  dwd_user_capture_detail
                      where toDate(capture_time-21600) =  %(date)s
                    ) v2
                    ON v1.profile_id = v2.profile_id
                   AND toDate(v1.capture_time-21600)   = toDate(v2.capture_time-21600)
                   where abs(dateDiff('minute', v1.capture_time, v2.capture_time)) <= 60
                and off_bin IN [-60, -45, -30, -15, -10, -5, 0, 5, 10, 15, 30, 45, 60]
                GROUP BY date_casino,date_hour,v1.region_name, z0_time, off_bin,v1.region_name_heatmap
                            , v1.member_tier , v1.profile_type , age_range, v1.gender 

        """

        ch = ClickHouseHandler(host=self.host, port=self.port, user=self.user, password=self.password,
                               database=self.database, prefix=self.target_table)

        for date in self.date_list:
            ch.delete_partition(delete_sql, self.target_table, {"date": date})
            # ch.stream_query_insert(source_sql, self.target_table,{"date":date},batch_size=100000)
            ch._insert_into_select(source_sql, self.target_table, {"date": date})
            # ch.stream_query_insert(source_sql, target_table,{"date":date},1000)


if __name__ == "__main__":
    host = ['localhost', 'localhost']
    port = [9000, 9000]
    user = ['default', 'default']
    password = ['ck_test', 'ck_test']
    database = ['Facial', 'Facial']
    target_table = ["dwd_user_capture_detail", "dws_profileid_group","dws_visitor_path_track_heatmap"]
    # date_list = ["2025-10-26", "2025-10-27", "2025-10-28"]
    date_list = pd.date_range("2025-10-01", "2025-10-31").strftime("%Y-%m-%d").tolist()
    pth = dws_visitor_path_track_heatmap(host=host, port=port, user=user, password=password, database=database,
                               target_table=target_table[2], date_list=date_list)
    pth.main()






