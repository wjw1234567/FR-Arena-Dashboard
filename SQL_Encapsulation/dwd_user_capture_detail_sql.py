from datetime  import datetime
# from ClickHouseHandler import ClickHouseHandler
from ClickHouseHandler_stream import ClickHouseHandler
import pandas as pd

class dwd_user_capture_detail:

    def __init__(self, host=["localhost","localhost"], port=[9000,9000], user=["default","default"], password=["",""], database=["default","default"],target_table=None,date_list=[]):

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



        delete_sql = f"alter table {self.target_table} delete where toDate(capture_time-21600)=%(date)s"

        source_sql = f"""
                     select
               profile_id
              , person_id
              ,profile_type
              ,member_tier
              ,member_id
              ,is_delete
              ,person_status
              ,album_id
              ,merge_count
              ,face_count
              ,identify_num
              ,card_type
              ,address
              ,name
              ,age
              ,gender
              ,capture_id
              ,region_id
              ,region_name
              ,sector
              ,region_name_heatmap
              ,region_type
              ,property
              ,camera_id
              ,capture_time
              ,next_capture_time
             , now() batch_time
            from
        (
            select
                  person_id
              ,camera_id
              ,capture_time
              , profile_id
              ,profile_type
              ,member_tier
              ,member_id
              ,is_delete
              ,person_status
              ,album_id
              ,merge_count
              ,face_count
              ,identify_num
              ,card_type
              ,address
              ,name
              ,age
              ,gender
              ,capture_id
              ,region_id
              ,region_name
              ,sector
              ,region_name_heatmap
              ,case when region_name like 'BW%%' then 'BW' else 'GF' end property
              ,case when region_name like 'CASINO%%' then '4' else '3' end region_type
             , leadInFrame(toNullable(capture_time),1,toNullable(null)) over(partition by (toDate(capture_time),t1.profile_id)  ORDER BY profile_id,capture_time RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) next_capture_time
              , lagInFrame(region_id,1,'0') over(partition by (toDate(capture_time),t1.profile_id) order by profile_id,capture_time) last_region_id
        from dwd_user_capture_original t1
        where toDate(t1.capture_time-21600)=%(date)s
            order by profile_id,capture_time
            ) A1  order by profile_id,capture_time
    
        """



        ch = ClickHouseHandler(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,prefix=self.target_table)

        for date in self.date_list:

            ch.delete_partition(delete_sql, self.target_table,{"date":date})
            # ch.stream_query_insert(source_sql, self.target_table,{"date":date},batch_size=10000)
            ch._insert_into_select(source_sql, self.target_table, {"date": date})



if __name__ == "__main__":
    # 读的IP和写的IP
    host = ['localhost', 'localhost']
    port = [9000, 9000]
    user = ['default', 'default']
    password = ['ck_test', 'ck_test']
    database = ['Facial', 'Facial']

    target_table = ["dwd_user_capture_detail"
        , "dws_profileid_group"
        , "dws_profileid_staytime"
        , "dws_profileid_NewOrReturn_visitor"
        , "dws_visitation_analytics_and_casino_entrances"
        , "dws_visitation_demographics"
        , "dws_visitor_path_track_heatmap"
                    ]


    date_list=pd.date_range("2025-08-01", "2025-08-01").strftime("%Y-%m-%d").tolist()


    cd=dwd_user_capture_detail(host=host, port=port, user=user, password=password, database=database,target_table=target_table[0],date_list=date_list)
    cd.main()






