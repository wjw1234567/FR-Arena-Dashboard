
# from dwd_user_capture_original_sql1 import dwd_user_capture_original
from dwd_user_capture_detail_sql import dwd_user_capture_detail
from dws_profileid_groupid_sql  import dws_profileid_group
from dws_profileid_NewOrReturn_visitor_sql import dws_profileid_NewOrReturn_visitor
from dws_profileid_staytime_sql import dws_profileid_staytime

from dws_profileid_all_index_sql import dws_profileid_all_index
from dws_visitation_daily_casino_sql import dws_visitation_daily_casino
from dws_visitation_daily_sql import dws_visitation_daily
from dws_visitation_group_casino_sql import dws_visitation_group_casino
from dws_visitation_hourly_casino_sql import dws_visitation_hourly_casino
from dws_visitation_hourly_sql import dws_visitation_hourly


# from dws_visitor_path_track_heatmap_sql import dws_visitor_path_track_heatmap
from dws_visitor_path_track_heatmap_new_sql import dws_visitor_path_track_heatmap
from dws_visitor_path_track_heatmap_daily_sql import dws_visitor_path_track_heatmap_daily
from frs_alias_cameraname_mapping import frs_alias_cameraname_mapping
from dws_visitation_daily_sector_sql import dws_visitation_daily_sector
from dws_visitation_hourly_sector_sql import dws_visitation_hourly_sector



# from dws_visitor_path_track_heatmap import TrackHeatmap
import schedule
import time
import pandas as pd
from datetime import datetime,timedelta
import logging

def run_jobs():

        # execute today
    # date_lists = pd.date_range((datetime.now()-timedelta(days=7)).strftime('%Y-%m-%d'), datetime.now().strftime('%Y-%m-%d')).strftime("%Y-%m-%d").tolist()

    # execute History Data
    date_lists=pd.date_range("2026-04-01", "2026-05-31").strftime("%Y-%m-%d").tolist()

    host_origin=['localhost','localhost']
    port_origin = [9000, 9000]
    user_origin = ['default', 'default']
    password_origin = ['', '']
    database_origin=['Facial', 'Facial']
        
    host = ['localhost', 'localhost']
    port = [9000, 9000]
    user = ['default', 'default']
    password = ['ck_test', 'ck_test']
    database = ['Facial', 'Facial']


    task_list=[

         # (dwd_user_capture_original,'dwd_user_capture_original'),
           (dwd_user_capture_detail,'dwd_user_capture_detail'),
        (dws_profileid_group,'dws_profileid_group'),
        (dws_profileid_staytime,'dws_profileid_staytime'),
        (dws_profileid_NewOrReturn_visitor,'dws_profileid_NewOrReturn_visitor'),
        (dws_profileid_all_index,'dws_profileid_all_index'),
        (dws_visitation_daily,'dws_visitation_daily'),
        (dws_visitation_daily_casino,'dws_visitation_daily_casino'),
        (dws_visitation_group_casino,'dws_visitation_group_casino'),
        (dws_visitation_hourly,'dws_visitation_hourly'),
        (dws_visitation_hourly_casino,'dws_visitation_hourly_casino')
       ,(dws_visitor_path_track_heatmap,'dws_visitor_path_track_heatmap')
       ,(frs_alias_cameraname_mapping,'frs_alias_cameraname_mapping')  
        ,(dws_visitation_daily_sector,'dws_visitation_daily_sector')
        ,(dws_visitation_hourly_sector,'dws_visitation_hourly_sector')
     ]






    for idx,(task_class,target_table) in enumerate(task_list,1):

        try:

            task_instance=task_class(host=host
                                     , port=port
                                     , user=user
                                     , password=password
                                     ,database=database
                                     ,target_table=target_table
                                     , date_list=date_lists)
            print(f"target_table={target_table}")
            task_instance.main()
            
            logging.info(f"==={idx}/{len(task_list)} execute success:{target_table}===")
        except Exception as e:
            # logging.error(f"==={idx}/{len(task_list)} execute fail:{target_table}")
            print(e)
            
    
    


    

    
    
    # '''
    #     热力图有2种实现方法SQL和Pandas
    # '''


    # pth = dws_visitor_path_track_heatmap(host=host, port=port, user=user, password=password, database=database,target_table=target_table[10], date_list=date_list)
    # pth.main()


    # trackheatmap = TrackHeatmap(host=host, port=port, user=user, password=password, database=database, prefix=target_table[10])
    # trackheatmap.process_main(target_table[10], date_list)



def main():
    # 每4小时执行一次 run_jobs 函数
    # schedule.every(4).hours.do(run_jobs)
    #
    # # 循环监听定时任务
    # while True:
    #     schedule.run_pending()  # 运行所有待执行的任务
    #     time.sleep(60)  # 每60秒检查一次是否有任务需要执行

    run_jobs()



if __name__ == "__main__":
    main()




