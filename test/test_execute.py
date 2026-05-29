import pandas as pd
import numpy as np
from clickhouse_driver import Client


class test_execute:
    def __init__(self, host=['localhost','localhost'], port=[9000,9000], user=['default','default'], password=['',''], database=['default','default'],prefix=None):

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

        self.client = Client(host=host[0], port=port[0], user=user[0], password=password[0], database=database[0])
        self.wd_client = Client(host=host[1], port=port[1], user=user[1], password=password[1], database=database[1])


    def main(self):

        list=[]
        sql=f"select camera_id from Facial.dwd_user_capture_detail where toDate(capture_time)='2025-08-01' limit 10"
        # for row in self.client.execute_iter(sql):
        #     print(row[0])
        result=self.client.execute(sql)
        tuple_re=tuple(t[0] for t in result if t)
        print(tuple_re)




        return

if __name__ == "__main__":
    host = ['localhost', 'localhost']
    port = [9000, 9000]
    user = ['default', 'default']
    password = ['ck_test', 'ck_test']
    database = ['Facial', 'Facial']
    target_table = ["dwd_user_capture_detail", "dws_profileid_group"]
    date_list=pd.date_range('2025-08-26','2025-08-28').strftime('%Y-%m-%d').to_list

    te=test_execute(host=host, port=port, user=user, password=password, database=database)
    te.main()




