from datetime  import datetime
# from ClickHouseHandler import ClickHouseHandler
from ClickHouseHandler_stream import ClickHouseHandler
import pandas as pd

class dim_arena_event_list:

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



        delete_sql = f" truncate {self.target_table}"

        source_sql = f"""
                  SELECT
                    toDate(`StartDtm_`) `CDate`
                    ,  concat(Title,'_',toString(toDate(`StartDtm_`))) as `Title`
                     , Venue
                     , EventType
                     , Genre
                     , StartDtm
                     , EndDtm
                     ,if(num=0,
                        StartDtm,
                        toDateTime(addDays(StartDtm,num))
                
                     ) `StartDtm_`
                
                     ,if(num=dateDiff('day', StartDtm, EndDtm)
                         ,EndDtm
                         ,toDateTime(addDays(StartDtm,num+1)) - INTERVAL 1 SECOND
                         )   `EndDtm_`
                
                    , case when toHour(`StartDtm_`) between '0' and '4' then 'EM'
                            when toHour(`StartDtm_`) between '5' and '12' then 'AM'
                            when toHour(`StartDtm_`) between '13' and '18' then 'PM'
                            when toHour(`StartDtm_`) between '19' and '24' then 'EV'
                         end  Period_Flag
                    , InsertDtm
                    , now() batch_time
                FROM
                (
                    SELECT
                        toDate(StartDtm) `CDate`
                     , Title
                     , StartDtm
                     , EndDtm
                     , Venue
                     , EventType
                     , Genre
                     , InsertDtm
                     , case when toHour(StartDtm) between '0' and '4' then 'EM'
                            when toHour(StartDtm) between '5' and '12' then 'AM'
                            when toHour(StartDtm) between '13' and '18' then 'PM'
                            when toHour(StartDtm) between '19' and '24' then 'EV'
                         end  Period_Flag
                       , arrayMap(x -> x, range(toInt64(dateDiff('day', StartDtm, EndDtm) + 1))) AS day_array
                    FROM Facial.dim_event_list t1
                )
                ARRAY JOIN day_array AS num
                order by `CDate`
                limit 1 by `CDate`,Period_Flag
    
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

    target_table = ["dim_arena_event_list"]


    date_list=pd.date_range("2025-08-01", "2025-08-01").strftime("%Y-%m-%d").tolist()


    cd=dim_arena_event_list(host=host, port=port, user=user, password=password, database=database,target_table=target_table[0],date_list=date_list)
    cd.main()






