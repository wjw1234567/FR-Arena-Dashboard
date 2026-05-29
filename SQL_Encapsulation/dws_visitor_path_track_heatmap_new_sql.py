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
            toDate(date_casino) AS date_casino,
            date_hour,
            target_bin AS off_bin,
        
            /* 核心逻辑调整 */
            
            
            multiIf(
            
            
                off_bin=0,region_name,

                has(movements.1, toInt64(target_bin)),
                movements.2[indexOf(movements.1, toInt64(target_bin))],

        -- 2. 默认留存：如果最大的抓拍记录就是0（说明后续没在别处出现），且当前是“后60分钟”
                 not has(movements.1, toInt64(target_bin)) and  arrayMax(movements.1) = 0 AND target_bin > 0,region_name,
                 not has(movements.1, toInt64(target_bin)) and  arrayMin(movements.1) = 0 AND target_bin < 0,region_name,
                 ''
                 
            )  AS area_at_off,
        
            
            
            multiIf(
                target_bin=0,region_name_heatmap,
                has(movements.1, toInt64(target_bin)),movements.3[indexOf(movements.1, toInt64(target_bin))],
                 not has(movements.1, toInt64(target_bin)) and arrayMax(movements.1) = 0 AND target_bin > 0,region_name_heatmap,
                 not has(movements.1, toInt64(target_bin)) and  arrayMin(movements.1) = 0 AND target_bin < 0,region_name_heatmap,
                ''
            )  AS area_at_off_heatmap,
        
            -- 统计在该时刻确认为“在场”的人数
            count(distinct if(area_at_off != '', profile_id, null)) AS visitor_num,
        
            region_name ,
            z0_time,
            member_tier,
            profile_type,
            age_range,
            gender,
            now() AS batch_time
        FROM
        (
            SELECT
                v1.profile_id profile_id,
                v1.capture_time capture_time,
                toDate(v1.capture_time - 21600) AS date_casino,
                formatDateTime(v1.capture_time, '%%H:00') AS date_hour,
                toStartOfHour(v1.capture_time) AS z0_time,
                v1.region_name region_name,
                v1.region_name_heatmap region_name_heatmap,
                v1.member_tier member_tier,
                v1.profile_type profile_type,
                CASE
                    WHEN v1.age BETWEEN 0 AND 20 THEN '0-20'
                    WHEN v1.age BETWEEN 21 AND 39 THEN '21-39'
                    WHEN v1.age BETWEEN 40 AND 65 THEN '40-65'
                    WHEN v1.age > 65 THEN '65+'
                END AS age_range,
                v1.gender gender,
        
                /* 生成轨迹数组 */
                arrayFilter(
                    x -> x.1 IS NOT NULL,
                    groupArray(
                        (
                            CASE
                                WHEN v2.capture_time IS NULL THEN NULL
                                WHEN v1.capture_time != v2.capture_time AND dateDiff('minute', v1.capture_time, v2.capture_time) BETWEEN 1 AND 15
                                    THEN (intDiv(dateDiff('minute', v1.capture_time, v2.capture_time), 5) + 1) * 5
                                WHEN dateDiff('minute', v1.capture_time, v2.capture_time) BETWEEN -15 AND -1
                                    THEN (intDiv(dateDiff('minute', v1.capture_time, v2.capture_time), 5) - 1) * 5
                                WHEN dateDiff('minute', v1.capture_time, v2.capture_time) BETWEEN 16 AND 60
                                    THEN (intDiv(dateDiff('minute', v1.capture_time, v2.capture_time), 15) + 1) * 15
                                WHEN dateDiff('minute', v1.capture_time, v2.capture_time) BETWEEN -60 AND -16
                                    THEN (intDiv(dateDiff('minute', v1.capture_time, v2.capture_time), 15) - 1) * 15
                                WHEN dateDiff('minute', v1.capture_time, v2.capture_time) = 0 THEN 0
                                ELSE NULL
                            END,
                            v2.region_name,
                            v2.region_name_heatmap
                        )
                    )
                ) AS movements
            FROM
            (
                -- V1: 基准数据
                SELECT profile_id, capture_time, member_tier, age, gender, profile_type,
                    CASE WHEN property='BW' THEN 'BW' WHEN region_type='4' THEN 'CASINO' ELSE region_name END AS region_name,
                    CASE WHEN property='BW' THEN 'BW' WHEN region_type='4' THEN 'CASINO' ELSE region_name_heatmap END AS region_name_heatmap
                FROM Facial.dwd_user_capture_detail
                WHERE toDate(capture_time - 21600) = %(date)s
            ) v1
            LEFT JOIN
            (
                -- V2: 轨迹数据
                SELECT profile_id, capture_time,
                    CASE WHEN property='BW' THEN 'BW' WHEN region_type='4' THEN 'CASINO' ELSE region_name END AS region_name,
                    CASE WHEN property='BW' THEN 'BW' WHEN region_type='4' THEN 'CASINO' ELSE region_name_heatmap END AS region_name_heatmap
                FROM Facial.dwd_user_capture_detail
                WHERE toDate(capture_time - 21600) = %(date)s
            ) v2
            ON v1.profile_id = v2.profile_id
            WHERE abs(dateDiff('minute', v1.capture_time, v2.capture_time)) <= 60
            GROUP BY
                v1.profile_id, v1.capture_time, v1.region_name, v1.region_name_heatmap,
                v1.member_tier, v1.profile_type, v1.age, v1.gender
        ) AS agg_table
        
        ARRAY JOIN [-60, -45, -30, -15, -10, -5, 0, 5, 10, 15, 30, 45, 60] AS target_bin
        
        GROUP BY
            date_casino, date_hour, off_bin, area_at_off, area_at_off_heatmap,
            region_name, z0_time, member_tier, profile_type, age_range, gender
        HAVING area_at_off != ''
        
       
        
                    
        
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
    date_list = pd.date_range("2025-08-01", "2025-08-31").strftime("%Y-%m-%d").tolist()
    pth = dws_visitor_path_track_heatmap(host=host, port=port, user=user, password=password, database=database,
                               target_table=target_table[2], date_list=date_list)
    pth.main()






