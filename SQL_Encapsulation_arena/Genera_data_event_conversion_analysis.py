#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
生成 dws_event_conversion_analysis 表的测试数据（约10万条）
"""
import random
import hashlib
from datetime import datetime, timedelta
from clickhouse_driver import Client
import sys

# 设置标准输出编码为 UTF-8
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

# ClickHouse连接信息
HOST = 'localhost'
PORT = 9000
DATABASE = 'Facial'
USER = 'default'
PASSWORD = 'ck_test'
TABLE_NAME = 'dws_event_conversion_analysis'

# 数据生成配置
TOTAL_RECORDS = 100000  # 总记录数
BATCH_SIZE = 5000  # 每批插入数量

# 事件名称列表
EVENT_NAMES = [
    "Youth Science and Technology Village Program 2024/2025",
    "BSL Winter Seasonal Menu",
    "Pak Loh CNY Pudding",
    "TERRAZZA Macallan Whisky Dinner",
    "GEG Responsible Gaming Kiosk Training",
    "Hall 7 new brand(s)",
    "LL flash mob at Music Chamber",
    "LONG BAR Guest Mixologist",
    "RG Community Promotions (Coloane)",
    "RG Knowledge Quiz 2025",
    "YAP \"Youth Achievement Program\" 2024/2025",
    "Tiffany workshop",
    "Macallan Whisky Dinner",
    "RG Bi-Yearly Refresher (Online) Training",
    "Branded event",
    "Raffles Wine Dinner",
    "DFS pop up"
]

# 入口组列表
GROUPS = [

    "GF Arena Hotel Entrance",
    "1F Event Bridge",
    "G3-G4 Lift Lobby",
    "JW Hotel Drop-off Ent.",
    "H10-H11 Lift Lobby",
    "Portuguese Bistro",
    "GF Arena Corporate Entrance",
    "Crystal Lobby",
    "H24-H25 Lift Lobby",
    "1F Event Retail",
    "GF VIP Entrance",
    "1F Event Escalator (R1/R2)",
    "JW Hotel Drop-off bays Ent.",
    "Diamond Lobby Exit 2",
    "East Square",
    "G7-G8 Lift Lobby",
    "GF Arena",
    "Sky Casion Lobby",
    "G3-G4 Retail Lift Lobby",
    "1F Event Escalator (R3/R4)",
    "Gaga Café",
    "GF Capella Property Ent.",
    "LG2 GICC",
    "GF Arena Door D",
    "Diamond Lobby",
    "Jade Lobby",
    "GF Arena Door A",
    "H1-H2 Lift Lobby",
    "GF Hotel",
    "G9-G10 Lift Lobby",
    "GF VIP Entrance",
    "China Rouge Ent.",
    "Banyan Tree",
    "Opal Lobby",
    "GF Arena Door F",
    "GM VIP Entrance",
    "G5-G6 Lift Lobby",
    "GF",
    "JW Hotel lobby Ent.",
    "Shroff Office",
    "GF Black & Diamond Lounge",
    "Okura Lobby",
    "B1",
    "1F Event Bridge & Retail",
    "Pearl Lobby",
    "LG1 G17-G20 Lift Lobby",
    "VIP East Entrance",
    "LG2 Guest Lift Lobby",
    "East Square Main Ent.",
    "RC Hotel lobby Ent.",
    "GM/BW Link Bridge",
    "MF Event BW/GM Link Bridge",
    "GM VIP Ent. Lobby 2",
    "DFS Ent. (Opal Lobby)",
    "G17-G20 Lift Lobby"
]

# 年龄区间列表
AGE_RANGES = ["65+", "21-39", "0-20", "40-65"]

# 会员等级列表
MEMBER_TIERS = ["Diamond", "Gold", "Basic", "Silver"]

# 日期范围
START_DATE = datetime(2026, 2, 1)
END_DATE = datetime(2026, 2, 28)


def generate_event_id(event_name):
    """生成事件ID（MD5哈希值）"""
    return hashlib.md5(event_name.encode('utf-8')).hexdigest()


def random_date(start, end):
    """生成随机日期"""
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days)


def random_datetime(start, end):
    """生成随机日期时间"""
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)


def generate_record():
    """生成一条记录"""
    # 随机选择事件名称
    event_name = random.choice(EVENT_NAMES)
    event_id = generate_event_id(event_name)

    # 随机生成事件日期
    event_date = random_date(START_DATE, END_DATE)

    # 随机选择其他字段
    group = random.choice(GROUPS)
    time_window_hours = random.choice([1, 2, 4])
    gender = random.choice([1, 2])
    age_range = random.choice(AGE_RANGES)
    profile_type = random.choice([1, 2, 3, 4])

    # member_tier 规则：仅当 profile_type=1 时从列表选择，否则为 "N/A"
    if profile_type == 1:
        member_tier = random.choice(MEMBER_TIERS)
    else:
        member_tier = "N/A"

    # 生成访问人数（30000-50000）
    visit_count = random.randint(30000, 50000)

    # casino_converted_people: visit_count 的 40%-70%
    conversion_ratio = random.uniform(0.4, 0.7)
    casino_converted_people = int(visit_count * conversion_ratio)

    # pre_visit_count: visit_count 的 40%-70%
    pre_ratio = random.uniform(0.4, 0.7)
    pre_visit_count = int(visit_count * pre_ratio)

    # post_visit_count: visit_count 的 40%-70%
    post_ratio = random.uniform(0.4, 0.7)
    post_visit_count = int(visit_count * post_ratio)

    # pre_event_casino_staytime: pre_visit_count × (90-180分钟)
    pre_staytime_per_visit = random.uniform(90, 180)
    pre_event_casino_staytime = round(pre_visit_count * pre_staytime_per_visit, 2)

    # post_event_casino_staytime: post_visit_count × (90-180分钟)
    post_staytime_per_visit = random.uniform(90, 180)
    post_event_casino_staytime = round(post_visit_count * post_staytime_per_visit, 2)

    # batch_time: 需晚于 event_date
    batch_time_start = datetime.combine(event_date, datetime.min.time()) + timedelta(hours=1)
    batch_time = random_datetime(batch_time_start, END_DATE + timedelta(days=1))

    return [
        event_date.date(),
        event_id,
        event_name,
        group,
        time_window_hours,
        gender,
        age_range,
        profile_type,
        member_tier,
        visit_count,
        casino_converted_people,
        pre_event_casino_staytime,
        post_event_casino_staytime,
        pre_visit_count,
        post_visit_count,
        batch_time
    ]


def main():
    print("=" * 80)
    print("ClickHouse 测试数据生成工具")
    print(f"目标表: {DATABASE}.{TABLE_NAME}")
    print(f"生成数量: {TOTAL_RECORDS:,} 条")
    print("=" * 80)

    # 连接 ClickHouse
    print(f"\n[1/4] 连接 ClickHouse...")
    print(f"  服务器: {HOST}:{PORT}")
    print(f"  数据库: {DATABASE}")

    try:
        client = Client(
            host=HOST,
            port=PORT,
            user=USER,
            password=PASSWORD,
            database=DATABASE
        )
        result = client.execute('SELECT version()')
        print(f"  ✓ 连接成功 (ClickHouse 版本: {result[0][0]})")
    except Exception as e:
        print(f"  ✗ 连接失败: {e}")
        sys.exit(1)

    # 检查表是否存在，如果不存在则创建
    print(f"\n[2/4] 检查表结构...")
    try:
        # 尝试查询表
        client.execute(f"SELECT 1 FROM {TABLE_NAME} LIMIT 1")
        print(f"  ✓ 表 {TABLE_NAME} 已存在")

        # 询问是否清空表
        print(f"\n  是否清空表 {TABLE_NAME}? (y/n): ", end='')
        # 自动清空
        print("y (自动)")
        client.execute(f"TRUNCATE TABLE {TABLE_NAME}")
        print(f"  ✓ 表已清空")

    except Exception as e:
        print(f"  表不存在，正在创建...")
        create_table_sql = f"""
        CREATE TABLE {TABLE_NAME} (
            event_date Date,
            event_id String,
            event_name String,
            `group` String COMMENT 'top entrances include groups and all',
            time_window_hours Int8 COMMENT 'time range [(-1,1),(-2,2),(-3,3)]',
            gender Int8,
            Age_range String,
            profile_type Int64,
            member_tier String,
            visit_count Int32,
            casino_converted_people Int32,
            pre_event_casino_staytime Float64,
            post_event_casino_staytime Float64,
            pre_visit_count Int32,
            post_visit_count Int32,
            batch_time DateTime
        ) ENGINE = MergeTree()
        PARTITION BY toMonth(event_date)
        ORDER BY (event_date)
        """
        try:
            client.execute(create_table_sql)
            print(f"  ✓ 表 {TABLE_NAME} 创建成功")
        except Exception as create_error:
            print(f"  ✗ 创建表失败: {create_error}")
            sys.exit(1)

    # 生成数据
    print(f"\n[3/4] 生成测试数据...")
    print(f"  总记录数: {TOTAL_RECORDS:,}")
    print(f"  批次大小: {BATCH_SIZE:,}")
    print(f"  预计批次: {TOTAL_RECORDS // BATCH_SIZE}")

    insert_sql = f"""
    INSERT INTO {TABLE_NAME} 
    (event_date, event_id, event_name, `group`, time_window_hours, gender, 
     Age_range, profile_type, member_tier, visit_count, casino_converted_people,
     pre_event_casino_staytime, post_event_casino_staytime, pre_visit_count, 
     post_visit_count, batch_time)
    VALUES
    """

    total_inserted = 0
    batch_count = 0

    try:
        while total_inserted < TOTAL_RECORDS:
            # 生成一批数据
            batch_data = []
            batch_size = min(BATCH_SIZE, TOTAL_RECORDS - total_inserted)

            for _ in range(batch_size):
                batch_data.append(generate_record())

            # 插入数据
            client.execute(insert_sql, batch_data)
            total_inserted += len(batch_data)
            batch_count += 1

            # 显示进度
            progress = (total_inserted / TOTAL_RECORDS) * 100
            print(f"  进度: {total_inserted:,}/{TOTAL_RECORDS:,} ({progress:.1f}%) - 批次 {batch_count}", end='\r')

        print(f"\n  ✓ 成功生成并插入 {total_inserted:,} 条数据")

    except Exception as e:
        print(f"\n  ✗ 插入失败: {e}")
        sys.exit(1)

    # 验证结果
    print(f"\n[4/4] 验证数据...")
    try:
        # 统计总数
        result = client.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        count = result[0][0]
        print(f"  表中总记录数: {count:,}")

        # 查询示例数据
        print(f"\n  示例数据（前3条）:")
        result = client.execute(f"""
            SELECT event_date, event_name, `group`, visit_count, casino_converted_people
            FROM {TABLE_NAME} 
            LIMIT 3
        """)
        for idx, row in enumerate(result, 1):
            print(f"    {idx}. 日期: {row[0]}, 事件: {row[1][:30]}..., 入口: {row[2][:20]}...")
            print(f"       访问人数: {row[3]:,}, 转化人数: {row[4]:,}")

        # 统计分析
        print(f"\n  数据统计:")

        # 按事件名称统计
        result = client.execute(f"""
            SELECT event_name, COUNT(*) as cnt
            FROM {TABLE_NAME}
            GROUP BY event_name
            ORDER BY cnt DESC
            LIMIT 5
        """)
        print(f"\n    事件名称分布（Top 5）:")
        for row in result:
            print(f"      {row[0][:40]}: {row[1]:,} 条")

        # 按时间窗口统计
        result = client.execute(f"""
            SELECT time_window_hours, COUNT(*) as cnt
            FROM {TABLE_NAME}
            GROUP BY time_window_hours
            ORDER BY time_window_hours
        """)
        print(f"\n    时间窗口分布:")
        for row in result:
            print(f"      {row[0]} 小时: {row[1]:,} 条")

        # 按性别统计
        result = client.execute(f"""
            SELECT gender, COUNT(*) as cnt
            FROM {TABLE_NAME}
            GROUP BY gender
            ORDER BY gender
        """)
        print(f"\n    性别分布:")
        for row in result:
            gender_name = "男性" if row[0] == 1 else "女性"
            print(f"      {gender_name}: {row[1]:,} 条")

        # 按档案类型统计
        result = client.execute(f"""
            SELECT profile_type, COUNT(*) as cnt
            FROM {TABLE_NAME}
            GROUP BY profile_type
            ORDER BY profile_type
        """)
        print(f"\n    档案类型分布:")
        for row in result:
            print(f"      类型 {row[0]}: {row[1]:,} 条")

        # 数值字段统计
        result = client.execute(f"""
            SELECT 
                AVG(visit_count) as avg_visit,
                AVG(casino_converted_people) as avg_converted,
                AVG(pre_event_casino_staytime) as avg_pre_time,
                AVG(post_event_casino_staytime) as avg_post_time
            FROM {TABLE_NAME}
        """)
        row = result[0]
        print(f"\n    数值字段平均值:")
        print(f"      平均访问人数: {row[0]:,.0f}")
        print(f"      平均转化人数: {row[1]:,.0f}")
        print(f"      平均事件前停留时长: {row[2]:,.2f} 分钟")
        print(f"      平均事件后停留时长: {row[3]:,.2f} 分钟")

        print("\n" + "=" * 80)
        print("✓ 测试数据生成完成！")
        print("=" * 80)

    except Exception as e:
        print(f"  ✗ 验证失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
