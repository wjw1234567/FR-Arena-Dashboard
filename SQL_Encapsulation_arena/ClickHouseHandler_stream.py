from clickhouse_driver import Client
from Logger import Logger


class ClickHouseHandler:
    def __init__(self, host=['localhost', 'localhost'], port=[9000, 9000],
                 user=['default', 'default'], password=['', ''],
                 database=['default', 'default'], prefix=None):
        # 存储配置，不再初始化长连接 client
        self.config = {
            'read': {
                'host': host[0], 'port': port[0],
                'user': user[0], 'password': password[0], 'database': database[0]
            },
            'write': {
                'host': host[1], 'port': port[1],
                'user': user[1], 'password': password[1], 'database': database[1]
            }
        }
        self.prefix = prefix
        self.logger = Logger(log_dir="./logs", prefix=self.prefix)

    def _get_new_client(self, mode='read'):
        """每次调用都会返回一个新的、干净的 Client 实例"""
        conf = self.config[mode]
        return Client(
            host=conf['host'],
            port=conf['port'],
            user=conf['user'],
            password=conf['password'],
            database=conf['database'],
            connect_timeout=20,  # 连接超时
            send_receive_timeout=300  # 读写超时，防止大数据量写入中断
        )

    def stream_query_insert(self, source_sql: str, target_table: str, condict: dict, batch_size: int = 10000):
        """流式查询并写入：读取使用一个连接，写入使用另一个连接"""
        # 1. 获取列名（内部已包含随用随关逻辑）
        column_names = self._get_query_columns(source_sql, condict)
        print(f"使用列名: {column_names}")

        # 2. 建立读取连接
        with self._get_new_client('read') as r_client:
            batch = []
            # execute_iter 在连接保持时持续产出数据
            for row in r_client.execute_iter(source_sql, params=condict):
                batch.append(row)
                if len(batch) >= batch_size:
                    self._insert_batch(target_table, column_names, batch)
                    batch.clear()

            if batch:
                self._insert_batch(target_table, column_names, batch)

    def _get_query_columns(self, sql: str, condict: dict):
        """短连接获取列名，执行完自动关闭"""
        with self._get_new_client('read') as client:
            res = client.execute(f"SELECT * FROM ({sql}) LIMIT 0", with_column_types=True, params=condict)
            columns = [col[0] for col in res[1]]
            return columns

    def delete_partition(self, delete_sql: str, table_name: str, condict: dict):
        """执行删除：随用随建，完工关闭"""
        try:
            with self._get_new_client('write') as client:
                print(f"执行删除: {delete_sql}")
                if not condict:
                    client.execute(delete_sql)
                else:
                    client.execute(delete_sql, params=condict)
                msg = f"{condict.get('date', '')} 已执行完成删除 {table_name} 的数据"
                print(msg)
                self.logger.log(msg)
        except Exception as e:
            self.logger.error(f"执行delete语句出错: {type(e).__name__}: {str(e)}")


    def _insert_batch(self, table_name: str, columns: list, data: list):
        """批量写入：随用随建，完工关闭"""
        if not data:
            return
        try:
            col_str = ', '.join(columns)
            with self._get_new_client('write') as client:
                client.execute(f"INSERT INTO {table_name} ({col_str}) VALUES", data)
            msg = f"已写入 {len(data)} 行到 {table_name}"
            print(msg)
            self.logger.log(msg)
        except Exception as e:
            self.logger.error(f"执行_insert_batch出错: {type(e).__name__}: {str(e)}")


    def _insert_into_select(self, source_sql: str, target_table: str, condit: dict):
        """INSERT INTO ... SELECT：随用随建，完工关闭"""
        try:
            # 1. 先获取列名
            column_names = self._get_query_columns(source_sql, condit)
            col_str = ', '.join(column_names)

            # 2. 执行写入
            with self._get_new_client('write') as client:
                sql = f"INSERT INTO {target_table} ({col_str}) {source_sql}"
                client.execute(sql, params=condit)
                self.logger.log(f"执行成功，已经写入 {target_table}")
            print(f"{condit.get('date', '')}已写入到 {target_table}")
        except Exception as e:
            self.logger.error(f"执行_insert_into_select出错: {type(e).__name__}: {str(e)}")

    def _get_date_list(self, source_sql: str, params: dict = None):
        try:

            with self._get_new_client('read') as client:
                # print(f"执行获取date_list): {source_sql}")
                if params is None:
                    result = client.execute(source_sql)
                elif  params is not None and not any(params.values() if isinstance(params, dict) else params):
                    self.logger.warning("传入的参数为空，跳过数据库查询。")
                    return []
                else:
                    result = client.execute(source_sql, params)
                if result:
                    return [row[0] for row in result]
                return []
        except Exception as e:
            self.logger.error(f"执行执行获取date_list语句出错: {type(e).__name__}: {str(e)}")
            return []


# ===================== 使用示例 =====================
if __name__ == "__main__":


    host = ['localhost', 'localhost']
    port = [9000, 9000]
    user = ['default', 'default']
    password = ['ck_test', 'ck_test']
    database = ['Facial', 'Facial']
    date_list = ["2025-08-25", "2025-08-26", "2025-08-27"]
    target_table = "dws_visitation_analytics_and_casino_entrances"

    ch = ClickHouseHandler(host=['localhost','localhost'], port=[9000,9000], user=['default','default'], password=['ck_test','ck_test'], database=['Facial','Facial'])

    delete_sql=f"alter table {target_table} delete where date=%(date)s"

    get_datelist_sql = f"""
                select distinct CDate from
            (select CDate,Title from dim_arena_event_list
            union all
            select CDate - interval 1 Day,Title from dim_arena_event_list
            union all
            select CDate + interval 1 Day ,Title from dim_arena_event_list) CD
            where CDate <= toDate(now())
            and Title in %(event_names)s
            order by  CDate desc 

    """

    get_event_sql = f"""

        select event_name from 
           ( select distinct Title event_name ,CDate from dim_arena_event_list
                 where CDate <= toDate(now()) order by  CDate desc )  limit 7
        """

    ch = ClickHouseHandler(host=host, port=port, user=user, password=password, database=database)
    event_list = ch._get_date_list(get_event_sql)
    print(event_list)

    ch = ClickHouseHandler(host=host, port=port, user=user, password=password, database=database)
    params = {'event_names':tuple(event_list)}
    date_lists = ch._get_date_list(get_datelist_sql, params)
    print(date_lists)

