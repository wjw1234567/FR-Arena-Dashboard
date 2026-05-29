from datetime  import datetime
# from ClickHouseHandler import ClickHouseHandler
from ClickHouseHandler_stream_old import ClickHouseHandler
import pandas as pd

# if __name__ == "__main__":

class dws_profileid_NewOrReturn_visitor:


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
        # target_table = "dws_profileid_NewOrReturn_cisitor"


        delete_sql = f"alter table  {self.target_table} delete where  date_casino=%(date)s"


        source_sql = f"""
                
   
    
                select               s1.date date
                         ,s1.date_casino date_casino
                         ,s1.profile_id profile_id
                         ,s1.region_id region_id
                         ,s1.region_name region_name
                         ,s1.region_type region_type
                         ,s1.gender gender
                         ,s1.Age_range Age_range
                         ,s1.profile_type profile_type
                         , s1.member_tier member_tier
                         ,s2.is_return is_return_zone
                         ,s3.is_return is_return_casino
                         ,case when s1.stay_time<15*60 then 1 else 0 end is_less15min_visitor
                         ,now() batch_time
    
                    from (select * from dws_profileid_staytime where date_casino= %(date)s) s1
                    
                     left join (select  profile_id,region_id,count(distinct profile_id) is_return from dws_profileid_staytime where region_type ='3' 
                                and date_casino>= toDate(%(date)s,'Asia/Shanghai') - interval 2 month 
                                and date_casino<  %(date)s  
                     group by profile_id,region_id) s2
                        on  s2.profile_id=s1.profile_id 
                        and s2.region_id=s1.region_id
                        
                     left join (select  profile_id,region_id,count(distinct profile_id) is_return from dws_profileid_staytime where region_type ='4'
                                  and date_casino>= toDate(%(date)s,'Asia/Shanghai') - interval 2 Day 
                                  and date_casino<  %(date)s  
                                 group by profile_id,region_id ) s3
                        on  s3.profile_id=s1.profile_id
                        and s3.region_id=s1.region_id
    
            """


        ch = ClickHouseHandler(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database,prefix=self.target_table)


        for date in self.date_list:

            ch.delete_partition(delete_sql, self.target_table,{"date":date})
            # ch.stream_query_insert(source_sql, self.target_table,{"date":date})
            ch._insert_into_select(source_sql, self.target_table,{"date":date})

if __name__ == "__main__":
    host = ['localhost', 'localhost']
    port = [9000, 9000]
    user = ['default', 'default']
    password = ['ck_test', 'ck_test']
    database = ['Facial', 'Facial']
    target_table = ["dwd_user_capture_detail", "dws_profileid_group","dws_profileid_NewOrReturn_visitor"]
    # date_list = ["2025-08-25", "2025-08-26", "2025-08-27"]
    date_list = pd.date_range("2025-10-01", "2025-10-31").strftime("%Y-%m-%d").tolist()

    pnv = dws_profileid_NewOrReturn_visitor(host=host, port=port, user=user, password=password, database=database,
                             target_table=target_table[2], date_list=date_list)
    pnv.main()





