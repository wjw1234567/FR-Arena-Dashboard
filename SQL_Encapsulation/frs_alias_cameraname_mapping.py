from datetime import datetime
# from ClickHouseHandler import ClickHouseHandler
from ClickHouseHandler_stream_old import ClickHouseHandler
import pandas as pd
from clickhouse_driver import Client
import logging
import numpy as np


# if __name__ == "__main__":


class frs_alias_cameraname_mapping:

    def __init__(self, host=["localhost", "localhost"], port=[9000, 9000], user=["default", "default"],
                 password=["", ""], database=["default", "default"], target_table=None, date_list=[]):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.target_table = target_table
        self.date_list = date_list

        self.read_client = Client(host=host[0], port=port[0], user=user[0], password=password[0], database=database[0])
        self.wd_client = Client(host=host[1], port=port[1], user=user[1], password=password[1], database=database[1])

    def main(self):
        get_camera_name_sql = f"""

        SELECT toString(camera_id) as camera_id ,camera_name  from  camera c       

        """

        get_alias_sql = f"""

        SELECT t2.property as property ,t2.alias as alias,toString(t1.camera_id)  as camera_id
             from frs_zone_cameras_mapping t1
        join frs_zones t2 on t1.zone_id =t2.zone_id    

        """

        rows_cam, cols_cam = self.read_client.execute(get_camera_name_sql, with_column_types=True)
        columns_cam = [c[0] for c in cols_cam]
        df_camera = pd.DataFrame(rows_cam, columns=columns_cam)

        rows_alias, cols_alias = self.wd_client.execute(get_alias_sql, with_column_types=True)
        columns_alias = [c[0] for c in cols_alias]
        df_alias = pd.DataFrame(rows_alias, columns=columns_alias)

        df_merge = pd.merge(
            left=df_camera,
            right=df_alias,
            on="camera_id",
            how="inner"

        )

        df_result = df_merge[["property", "alias", "camera_name"]]

        ch = ClickHouseHandler(host=self.host, port=self.port, user=self.user, password=self.password,
                               database=self.database, prefix=self.target_table)

        batch = [tuple(row) for row in df_result.values]

        ch._insert_batch(self.target_table, df_result.columns.tolist(), batch)


if __name__ == "__main__":
    host = ['localhost', 'localhost']
    port = [9000, 9000]
    user = ['default', 'default']
    password = ['ck_test', 'ck_test']
    database = ['Facial', 'Facial']
    date_list = pd.date_range("2025-08-01", "2025-08-01").strftime("%Y-%m-%d").tolist()

    target_table = [
        "frs_alias_cameraname_mapping"
        , "dwd_user_capture_detail"
        , "dws_profileid_group"
        , "dws_profileid_staytime"
        , "dws_profileid_NewOrReturn_visitor"
        , "dws_profileid_all_index"
        , "dws_visitation_demographics"
        , "dws_visitor_path_track_heatmap"
    ]
    # date_list = ["2025-08-25", "2025-08-26", "2025-08-27"]

    facm = frs_alias_cameraname_mapping(host=host, port=port, user=user, password=password, database=database,
                                        target_table=target_table[0], date_list=date_list)
    facm.main()






