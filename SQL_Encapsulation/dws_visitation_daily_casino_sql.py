from datetime import datetime
# from ClickHouseHandler import ClickHouseHandler
from ClickHouseHandler_stream_old import ClickHouseHandler
import pandas as pd

class dws_visitation_daily_casino:

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

        delete_sql = f"alter table  {self.target_table} delete where  date_casino =%(date)s"

        source_sql = f"""


                 with tab_profile as (
                    select toDate(date_casino_hour) date_casino
                    ,toDate(date_hour) date
              , profile_id
              , region_type
              , region_id
              , region_name
              , gender
              , Age_range
              , profile_type
              , member_tier
              , max(stay_time) stay_time_p
              , case when region_type='3'and max(stay_time) < 15*60 then 1 else 0 end is_less_15min_region
              , case when region_type='4'and max(stay_time) < 15*60 then 1 else 0 end is_less_15min_casino
              , is_return_zone
              , is_return_casino
        from  dws_profileid_all_index
        where toDate(date_casino_hour)= %(date)s
        group by date,date_casino,profile_id, region_type, region_id, region_name, gender, Age_range, profile_type, member_tier, is_return_zone, is_return_casino
    
             )
    
    
             select  date_casino
                , region_type
                , region_id
                , region_name
                , gender
                , Age_range
                , profile_type
                , member_tier
                , count(distinct profile_id) visitor_num
                , sum(stay_time_p)  stay_time
                , count(distinct case when is_less_15min_casino then profile_id end )  less15min_casino_visitor_num
                , count(distinct case when is_return_casino =1 then profile_id end )      return_casino_visitor_num
                , count(distinct case when is_return_casino =0 then profile_id end )      new_casino_visitor_num
                , sum( case when is_return_casino =1 then stay_time_p end )      return_casino_stay_time
                , sum( case when is_return_casino =0 then stay_time_p end )      new_casino_stay_time
                
                , count(distinct case when is_return_casino =1 and is_less_15min_casino=1 then profile_id end ) return_casino_less15min_num
                , count(distinct case when is_return_casino =0 and is_less_15min_casino=1 then profile_id end ) new_casino_less15min_num
           
                
                , now() batch_time
    
        from  tab_profile t1
        where  region_type = '4'
        group by date_casino, region_type, region_id, region_name, gender, Age_range, profile_type, member_tier

        """



        source_sql_all = f"""



      with tab_profile as (
                    select toDate(date_casino_hour) date_casino
                    ,toDate(date_hour) date
              , profile_id
              , region_type
              , region_id
              , region_name
              , gender
              , Age_range
              , profile_type
              , member_tier
              , max(stay_time) stay_time_p
              , case when region_type='3'and max(stay_time) < 15*60 then 1 else 0 end is_less_15min_region
              , case when region_type='4'and max(stay_time) < 15*60 then 1 else 0 end is_less_15min_casino
              , is_return_zone
              , is_return_casino
        from  dws_profileid_all_index
        where toDate(date_casino_hour)= %(date)s
         and  region_type = '4'
        group by date,date_casino,profile_id, region_type, region_id, region_name, gender, Age_range, profile_type, member_tier, is_return_zone, is_return_casino
    
             )


         ,tab_profile_staytime_all as (
           
            
            select 
               date_casino
               ,profile_id
               ,sum(stay_time_p)  stay_time_p_less15min_all
            
            from  tab_profile t1
            group by date_casino,profile_id    
            having stay_time_p_less15min_all < 15*60
         )


        ,tab_is_return_all as (
            select 
               date_casino
               ,profile_id        
               ,sum(is_return_casino)  is_return_casino_all
            from  tab_profile t1
            group by date_casino,profile_id   
            having   is_return_casino_all=0         
         )
    
    
             select  t1.date_casino date_casino
                ,  region_type
                , 'all' region_id
                , 'all' region_name
                , gender
                , Age_range
                , profile_type
                , member_tier
                , count(distinct t1.profile_id) visitor_num
                , sum(stay_time_p)  stay_time
                , count(distinct t2.profile_id  )  less15min_casino_visitor_num          


                ,count(distinct case when t3.profile_id =0  then t1.profile_id  end )   return_casino_visitor_num
                    
                ,count(distinct case when t3.profile_id <> 0  then t1.profile_id   end )   new_casino_visitor_num

                
                , sum( case when is_return_casino =1 then stay_time_p end )      return_casino_stay_time
                , sum( case when is_return_casino =0 then stay_time_p end )      new_casino_stay_time
               , count(distinct case  when is_return_casino =1  then t2.profile_id end ) return_casino_less15min_num
                , count(distinct case when is_return_casino =0  then t2.profile_id end ) new_casino_less15min_num
                
                , now() batch_time
    
        from  tab_profile t1
        left join tab_profile_staytime_all t2  on  t1.date_casino=t2.date_casino and t1.profile_id=t2.profile_id
        left join tab_is_return_all t3 on t1.profile_id=t3.profile_id and t1.date_casino=t3.date_casino 
       
        group by t1.date_casino, region_type, gender, Age_range, profile_type, member_tier

        """

        ch = ClickHouseHandler(host=self.host, port=self.port, user=self.user, password=self.password,
                               database=self.database, prefix=self.target_table)

        for date in self.date_list:
            ch.delete_partition(delete_sql, self.target_table, {"date": date})
            ch._insert_into_select(source_sql, self.target_table, {"date": date})
            ch._insert_into_select(source_sql_all, self.target_table, {"date": date})

            # ch.stream_query_insert(source_sql, target_table,{"date":date},1000)


if __name__ == "__main__":
    host = ['localhost', 'localhost']
    port = [9000, 9000]
    user = ['default', 'default']
    password = ['ck_test', 'ck_test']
    database = ['Facial', 'Facial']
    target_table = ["dwd_user_capture_detail", "dws_profileid_group", "dws_profileid_NewOrReturn_visitor",
                    "dws_profileid_staytime", "dws_visitation_analytics_and_casino_entrances",
                    "dws_visitation_hourly","dws_visitation_daily_casino"]
    # date_list = ["2025-08-25", "2025-08-26", "2025-08-27"]
    date_list = pd.date_range("2025-10-01", "2025-10-31").strftime("%Y-%m-%d").tolist()

    vd = dws_visitation_daily_casino(host=host, port=port, user=user, password=password, database=database,
                                     target_table=target_table[6], date_list=date_list)
    vd.main()





