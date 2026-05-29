from datetime  import datetime
# from ClickHouseHandler import ClickHouseHandler
from ClickHouseHandler_stream_old import ClickHouseHandler
import pandas as pd

# if __name__ == "__main__":


class dws_profileid_all_index:



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
        # date_list = ["2025-08-25", "2025-08-26", "2025-08-27"]
        # target_table = "dws_visitation_analytics_and_casino_entrances"





        delete_sql = f"alter table  {self.target_table} delete where  toDate(date_casino_hour)=%(date)s"

        source_sql = f"""
       
   with tab_profile as (
    select
           t1.date_hour date_hour
         , t1.date_casino_hour date_casino_hour
         , t1.profile_id profile_id
         , t1.region_type region_type
         , t1.region_id region_id
         , t1.region_name region_name
         , t1.gender gender
         , t1.Age_range Age_range
         , t1.profile_type profile_type
         , t1.member_tier member_tier
         , t1.capture_time capture_time     
         , t1.sector  sector
         , t3.stay_time stay_time_casino
         , t3.stay_time stay_time_zone
         , t4.is_less15min_visitor is_less15min_visitor
         , t4.is_return_casino is_return_casino
         , t4.is_return_zone is_return_zone
         , now() batch_time
    from (
        select
            toStartOfHour(capture_time) date_hour
         , toStartOfHour(capture_time-21600) date_casino_hour
         , profile_id profile_id
         , region_type region_type
         , region_id region_id
         , region_name region_name
         , gender gender
         , case when age between 0 and 20 then '0-20'
                                     when age between 21 and 39 then '21-39'
                                     when age between 40 and 65 then '40-65'
                                     when age >65 then  '65+'
                               end   Age_range
         , profile_type profile_type
         , member_tier member_tier
         , capture_time capture_time
         , next_capture_time
         , sector
        from
        dwd_user_capture_detail  where  toDate(capture_time-21600)= %(date)s) t1


         left join (select * from dws_profileid_staytime where date_casino=%(date)s) t3
                                         on toDate(t1.capture_time) = t3.date
                                              and toDate(t1.capture_time - 21600) = t3.date_casino
                                              and t1.profile_id=t3.profile_id
                                              and t1.region_id=t3.region_id
         left join (select * from dws_profileid_NewOrReturn_visitor where date_casino=%(date)s) t4 
                                             on toDate(t1.capture_time) = t4.date
                                              and toDate(t1.capture_time - 21600) = t4.date_casino
                                              and t1.profile_id=t4.profile_id
                                              and t1.region_id=t4.region_id
                                              
                    
                    )
                                              
                                              
       
     select     
           date_hour
          ,date_casino_hour
          ,profile_id
          ,region_id
          ,region_name
          ,region_type
          ,gender
          ,Age_range
          ,profile_type
          ,member_tier
          , sector
          ,max(stay_time_zone) stay_time
          ,case when  max(stay_time_zone) < 15*60 then 1 else 0 end is_less_15min_region
          ,case when  max(stay_time_zone) < 15*60 then 1 else 0 end is_less_15min_casino
          ,is_return_zone
          ,is_return_casino
          , now() batch_time
    
    from tab_profile
    group by  date_hour,date_casino_hour,profile_id,region_id,region_name,region_type,gender,Age_range,profile_type,member_tier,is_return_zone,is_return_casino,sector
                    
                               
    
        """

        ch = ClickHouseHandler(host=self.host, port=self.port, user=self.user, password=self.password,
                               database=self.database, prefix=self.target_table)

        for date in self.date_list:

            ch.delete_partition(delete_sql, self.target_table,{"date":date})
            # ch.stream_query_insert(source_sql, self.target_table,{"date":date},batch_size=100000)
            ch._insert_into_select(source_sql, self.target_table,{"date":date})


if __name__ == "__main__":

    host = ['localhost', 'localhost']
    port = [9000, 9000]
    user = ['default', 'default']
    password = ['ck_test', 'ck_test']
    database = ['Facial', 'Facial']
    target_table = ["dwd_user_capture_detail", "dws_profileid_group", "dws_profileid_NewOrReturn_visitor",
                    "dws_profileid_staytime","dws_profileid_all_index"]
    date_list = pd.date_range("2025-08-12", "2025-08-13").strftime("%Y-%m-%d").tolist()
    vase = dws_profileid_all_index(host=host, port=port, user=user, password=password, database=database,
                                target_table=target_table[4], date_list=date_list)
    vase.main()






