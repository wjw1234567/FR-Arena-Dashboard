#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Excel 数据导入 ClickHouse 通用工具
支持自动建表、数据类型转换、分批写入
"""
import pandas as pd
from clickhouse_driver import Client
import sys
from pathlib import Path


class ExcelToClickHouse:
    """Excel 到 ClickHouse 数据导入工具"""

    def __init__(self, host='localhost', port=9000, database='default',
                 user='default', password='', batch_size=5000):
        """
        初始化连接配置

        Args:
            host: ClickHouse 主机地址
            port: ClickHouse 端口
            database: 数据库名称
            user: 用户名
            password: 密码
            batch_size: 批量插入大小
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.batch_size = batch_size
        self.client = None

        # Windows 控制台 UTF-8 支持
        if sys.platform == 'win32':
            try:
                sys.stdout.reconfigure(encoding='utf-8')
            except:
                pass

    def connect(self):
        """连接 ClickHouse"""
        try:
            self.client = Client(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            version = self.client.execute('SELECT version()')[0][0]
            print(f"✓ 连接成功 (ClickHouse {version})")
            return True
        except Exception as e:
            print(f"✗ 连接失败: {e}")
            return False

    def read_excel(self, file_path, sheet_name=0):
        """
        读取 Excel 文件

        Args:
            file_path: Excel 文件路径
            sheet_name: 工作表名称或索引（默认第一个）

        Returns:
            DataFrame
        """
        try:
            print(f"\n读取 Excel: {file_path}")
            df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
            df = df.fillna('')  # 填充空值
            print(f"✓ 读取成功: {len(df)} 行 × {len(df.columns)} 列")
            print(f"  列名: {', '.join(df.columns.tolist())}")
            return df
        except Exception as e:
            print(f"✗ 读取失败: {e}")
            return None

    def create_table(self, table_name, columns, drop_if_exists=False):
        """
        根据列名创建 ClickHouse 表（所有字段为 String 类型）

        Args:
            table_name: 表名
            columns: 列名列表
            drop_if_exists: 是否删除已存在的表
        """
        try:
            # 删除旧表
            if drop_if_exists:
                self.client.execute(f"DROP TABLE IF EXISTS {table_name}")
                print(f"✓ 已删除旧表: {table_name}")

            # 构建建表语句
            column_defs = ',\n    '.join([f"`{col}` String" for col in columns])
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {column_defs}
            ) ENGINE = MergeTree()
            ORDER BY tuple()
            """

            self.client.execute(create_sql)
            print(f"✓ 表创建成功: {table_name}")
            return True
        except Exception as e:
            print(f"✗ 创建表失败: {e}")
            return False

    def insert_data(self, table_name, df):
        """
        分批插入数据到 ClickHouse

        Args:
            table_name: 表名
            df: DataFrame 数据
        """
        try:
            total_rows = len(df)
            columns = df.columns.tolist()

            # 构建插入语句
            column_names = ', '.join([f"`{col}`" for col in columns])
            insert_sql = f"INSERT INTO {table_name} ({column_names}) VALUES"

            # 转换数据为列表
            data = df.astype(str).values.tolist()

            # 分批插入
            print(f"\n开始插入数据...")
            inserted = 0

            for i in range(0, total_rows, self.batch_size):
                batch = data[i:i + self.batch_size]
                self.client.execute(insert_sql, batch)
                inserted += len(batch)
                progress = (inserted / total_rows) * 100
                print(f"  进度: {inserted:,}/{total_rows:,} ({progress:.1f}%)", end='\r')

            print(f"\n✓ 插入完成: {inserted:,} 行")
            return True
        except Exception as e:
            print(f"\n✗ 插入失败: {e}")
            return False

    def verify_data(self, table_name, df):
        """
        验证插入的数据

        Args:
            table_name: 表名
            df: 原始 DataFrame
        """
        try:
            print(f"\n验证数据...")

            # 统计总数
            result = self.client.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = result[0][0]
            print(f"  表中记录数: {count:,}")
            print(f"  原始记录数: {len(df):,}")

            if count == len(df):
                print(f"  ✓ 数据完整")
            else:
                print(f"  ⚠ 数据不完整")

            # 显示前3行
            print(f"\n  前3行数据:")
            columns = df.columns.tolist()
            column_names = ', '.join([f"`{col}`" for col in columns[:3]])
            result = self.client.execute(f"SELECT {column_names} FROM {table_name} LIMIT 3")

            for idx, row in enumerate(result, 1):
                print(f"    {idx}. {' | '.join(str(v)[:30] for v in row)}")

            return True
        except Exception as e:
            print(f"  ✗ 验证失败: {e}")
            return False

    def import_excel(self, excel_path, table_name, sheet_name=0, drop_if_exists=False):
        """
        一键导入 Excel 到 ClickHouse

        Args:
            excel_path: Excel 文件路径
            table_name: 目标表名
            sheet_name: 工作表名称或索引
            drop_if_exists: 是否删除已存在的表
        """
        print("=" * 70)
        print(f"Excel 导入 ClickHouse")
        print(f"文件: {excel_path}")
        print(f"目标表: {self.database}.{table_name}")
        print("=" * 70)

        # 连接数据库
        if not self.connect():
            return False

        # 读取 Excel
        df = self.read_excel(excel_path, sheet_name)
        if df is None:
            return False

        # 创建表
        if not self.create_table(table_name, df.columns.tolist(), drop_if_exists):
            return False

        # 插入数据
        if not self.insert_data(table_name, df):
            return False

        # 验证数据
        self.verify_data(table_name, df)

        print("\n" + "=" * 70)
        print("✓ 导入完成！")
        print("=" * 70)
        return True


def main():
    """主函数 - 使用示例"""

    # ========== 配置区域 ==========
    # ClickHouse 连接配置
    CONFIG = {
        'host': 'localhost',
        'port': 9000,
        'database': 'Facial',
        'user': 'default',
        'password': 'ck_test',
        'batch_size': 5000  # 每批插入行数
    }

    # Excel 文件路径
    # EXCEL_FILE = 'arena_event_sample (with genre)_new.xlsx'

    EXCEL_FILE = './ExcelFile/etl生成-权限标签表.xlsx'

    # 目标表名（自定义）
    TABLE_NAME = 'FineBi_etl_generate_permission_tag'

    # 工作表名称或索引（0 表示第一个工作表）
    SHEET_NAME = 0

    # 是否删除已存在的表
    DROP_IF_EXISTS = True
    # ========== 配置区域结束 ==========

    # 检查文件是否存在
    if not Path(EXCEL_FILE).exists():
        print(f"✗ 文件不存在: {EXCEL_FILE}")
        print(f"\n请修改脚本中的 EXCEL_FILE 变量为你的 Excel 文件路径")
        return

    # 创建导入工具实例
    importer = ExcelToClickHouse(**CONFIG)

    # 执行导入
    importer.import_excel(
        excel_path=EXCEL_FILE,
        table_name=TABLE_NAME,
        sheet_name=SHEET_NAME,
        drop_if_exists=DROP_IF_EXISTS
    )


if __name__ == "__main__":
    main()
