import time
from functools import wraps
from clickhouse_driver import Client
from clickhouse_driver.errors import Error as CKError


from Logger import Logger # 确保 Logger 导入正常

def retry_on_exception(retries=3, delay=5):
    """ClickHouse 操作重试装饰器"""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay
            for attempt in range(retries + 1):
                try:
                    return func(*args, **kwargs)
                except (CKError, Exception) as e:
                    last_exception = e
                    if attempt == retries:
                        break
                    print(f"⚠️ 操作异常: {str(e)}。正在重试({attempt + 1}/{retries})，等待 {current_delay}s...")
                    time.sleep(current_delay)
                    current_delay *= 2
            raise last_exception

        return wrapper

    return decorator


class ClickHouseHandler:
    def __init__(self, host=['localhost', 'localhost'], port=[9000, 9000],
                 user=['default', 'default'], password=['', ''],
                 database=['default', 'default'], prefix=None):
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
        """物理创建一个新的短连接，确保账号密码验证在每次任务开始时重新触发"""
        conf = self.config[mode]
        return Client(
            host=conf['host'], port=conf['port'],
            user=conf['user'], password=conf['password'], database=conf['database'],
            connect_timeout=20, send_receive_timeout=600
        )

    @retry_on_exception(retries=3)
    def delete_partition(self, delete_sql: str, table_name: str, condict: dict = None):
        """
        删除数据：增强了对空参数的兼容性
        """
        # 统一处理 condict 为 None 或 {} 的情况
        params = condict if condict else {}
        date_str = params.get('date', 'All/Unknown')

        try:
            with self._get_new_client('write') as client:
                print(f"🚀 正在执行删除 [{table_name}]，条件参数: {params}")

                if not params:
                    # 如果没有参数，直接执行 SQL（注意：这通常是全表删除或静态SQL）
                    client.execute(delete_sql)
                else:
                    # 带有参数化的执行
                    client.execute(delete_sql, params=params)

                success_msg = f"✅ {date_str} 数据清理完成 (Table: {table_name})"
                print(success_msg)
                self.logger.log(success_msg)
        except Exception as e:
            err_msg = f"❌ 执行删除出错: {type(e).__name__}: {str(e)}"
            print(err_msg)
            self.logger.error(err_msg)
            raise e

    def _get_query_columns(self, sql: str, condict: dict):
        """内部调用：获取列名"""
        params = condict if condict else {}
        with self._get_new_client('read') as client:
            res = client.execute(f"SELECT * FROM ({sql}) LIMIT 0", with_column_types=True, params=params)
            return [col[0] for col in res[1]]

    @retry_on_exception(retries=3)
    def stream_query_insert(self, source_sql: str, target_table: str, condict: dict = None, batch_size: int = 10000):
        """流式处理并写入"""
        params = condict if condict else {}
        column_names = self._get_query_columns(source_sql, params)

        with self._get_new_client('read') as r_client:
            batch = []
            for row in r_client.execute_iter(source_sql, params=params):
                batch.append(row)
                if len(batch) >= batch_size:
                    self._insert_batch(target_table, column_names, batch)
                    batch.clear()
            if batch:
                self._insert_batch(target_table, column_names, batch)

    @retry_on_exception(retries=3)
    def _insert_batch(self, table_name: str, columns: list, data: list):
        """物理写入"""
        if not data: return
        col_str = ', '.join(columns)
        with self._get_new_client('write') as client:
            client.execute(f"INSERT INTO {table_name} ({col_str}) VALUES", data)
            self.logger.log(f"📦 已成功批量写入 {len(data)} 行")
            print(f"📦 已成功批量写入 {len(data)} 行")

    @retry_on_exception(retries=3)
    def _insert_into_select(self, source_sql: str, target_table: str, condit: dict = None):
        """直接执行 INSERT INTO SELECT"""
        params = condit if condit else {}
        column_names = self._get_query_columns(source_sql, params)
        col_str = ', '.join(column_names)
        full_sql = f"INSERT INTO {target_table} ({col_str}) {source_sql}"

        with self._get_new_client('write') as client:
            client.execute(full_sql, params=params)
            self.logger.log(f"🚀 INSERT INTO SELECT 任务完成: {params.get('date', '')}")
            print(f"🚀 INSERT INTO SELECT 任务完成: {params.get('date', '')}")