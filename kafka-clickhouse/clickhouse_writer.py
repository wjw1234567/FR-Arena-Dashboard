"""
ClickHouse写入模块
"""
from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError
from datetime import datetime
import config


class ClickHouseWriter:
    def __init__(self):
        """初始化ClickHouse客户端"""
        self.client = None
        self._connect()
        self._ensure_table_exists()
    
    def _connect(self):
        """连接到ClickHouse"""
        try:
            self.client = Client(
                host=config.CLICKHOUSE_HOST,
                port=config.CLICKHOUSE_PORT,
                database=config.CLICKHOUSE_DATABASE,
                user=config.CLICKHOUSE_USER,
                password=config.CLICKHOUSE_PASSWORD
            )
            # 测试连接
            self.client.execute('SELECT 1')
            print(f"[ClickHouse写入器] 成功连接到 {config.CLICKHOUSE_HOST}:{config.CLICKHOUSE_PORT}")
        except Exception as e:
            print(f"[错误] ClickHouse连接失败: {e}")
            raise
    
    def _ensure_table_exists(self):
        """确保数据表存在，不存在则创建"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {config.CLICKHOUSE_TABLE} (
            date_time DateTime,
            ID UInt64
        ) ENGINE = MergeTree()
        ORDER BY ID
        """
        try:
            self.client.execute(create_table_sql)
            print(f"[ClickHouse写入器] 数据表 {config.CLICKHOUSE_TABLE} 已就绪")
        except ClickHouseError as e:
            print(f"[错误] 创建表失败: {e}")
            raise
    
    def _parse_datetime(self, date_str: str) -> datetime:
        """
        将字符串转换为datetime对象
        支持格式: "YYYY-MM-DD HH:MM:SS"
        """
        try:
            return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        except ValueError as e:
            print(f"[警告] 日期格式错误: {date_str}, 使用当前时间")
            return datetime.now()
    
    def write_batch(self, batch: list) -> bool:
        """
        批量写入数据到ClickHouse
        参数: batch - 数据列表，格式 [{"date_time": "...", "ID": ...}, ...]
        返回: 是否写入成功
        """
        if not batch:
            return True
        
        try:
            # 准备插入数据，将字符串转换为datetime对象
            data = [
                (self._parse_datetime(record['date_time']), record['ID']) 
                for record in batch
            ]
            
            # 批量插入
            insert_sql = f"INSERT INTO {config.CLICKHOUSE_TABLE} (date_time, ID) VALUES"
            self.client.execute(insert_sql, data)
            
            print(f"[ClickHouse写入器] 成功写入 {len(batch)} 条数据")
            return True
            
        except ClickHouseError as e:
            print(f"[错误] ClickHouse写入失败: {e}")
            return False
        except KeyError as e:
            print(f"[错误] 数据格式错误，缺少字段: {e}")
            return False
        except Exception as e:
            print(f"[错误] 未知错误: {e}")
            return False
    
    def query_count(self) -> int:
        """查询表中总记录数"""
        try:
            result = self.client.execute(f"SELECT COUNT(*) FROM {config.CLICKHOUSE_TABLE}")
            return result[0][0]
        except Exception as e:
            print(f"[错误] 查询失败: {e}")
            return -1
    
    def query_latest(self, limit: int = 10) -> list:
        """查询最新的N条记录"""
        try:
            result = self.client.execute(
                f"SELECT * FROM {config.CLICKHOUSE_TABLE} ORDER BY ID DESC LIMIT {limit}"
            )
            return result
        except Exception as e:
            print(f"[错误] 查询失败: {e}")
            return []
    
    def close(self):
        """关闭ClickHouse连接"""
        if self.client:
            self.client.disconnect()
            print("[ClickHouse写入器] 已关闭连接")


if __name__ == "__main__":
    # 测试ClickHouse写入器
    writer = ClickHouseWriter()
    
    # 测试写入
    test_data = [
        {"date_time": "2024-01-01 12:00:00", "ID": 1},
        {"date_time": "2024-01-01 12:00:01", "ID": 2}
    ]
    writer.write_batch(test_data)
    
    # 查询验证
    count = writer.query_count()
    print(f"表中总记录数: {count}")
    
    latest = writer.query_latest(5)
    print(f"最新5条记录: {latest}")
    
    writer.close()
