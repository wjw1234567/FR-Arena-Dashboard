import random
from datetime import datetime, timedelta
from clickhouse_driver import Client

# ---------------------
# 配置 ClickHouse 连接
# ---------------------
client = Client(
    host="localhost",  # 替换为你的 ClickHouse 地址
    port=9000,  # TCP 端口
    user="default",
    password="ck_test",
    database="Facial"
)


# ---------------------
# 数据生成器（yield）- 700用户/月300万条/全天0-23小时有记录/每小时停留3-20分钟
# ---------------------
def mock_data_generator(num_users=700, total_records=3000000, start_date="2025-08-01", days=31):
    regions = [
        ('1', 'GM GF P1A alias', 'Music', 'GM GF P1A'),
        ('2', 'GM GF P1B alias', 'Music', 'GM GF P1B'),
        ('3', 'GM GF P1C alias', 'Music', 'GM GF P1C'),
        ('4', 'GM GF P2A alias', 'Music', 'GM GF P2A'),
        ('5', 'GM GF P2B alias', 'Music', 'GM GF P2B'),
        ('6', 'GM GF P2C alias', 'Music', 'GM GF P2C'),
        ('7', 'GM GF P1D alias', 'Music', 'GM GF P1D'),
        ('8', 'GM GF P1E alias', 'Music', 'GM GF P1E'),
        ('9', 'GM GF P1F alias', 'Music', 'GM GF P1F'),
        ('10', 'GM GF P2D alias', 'Music', 'GM GF P2D'),
        ('11', 'GM GF P2E alias', 'Music', 'GM GF P2E'),
        ('12', 'GM GF P2F alias', 'Music', 'GM GF P2F'),
        ('13', 'GM GF P2G alias', 'Music', 'GM GF P2G'),
        ('14', 'GM GF P2H alias', 'Music', 'GM GF P2H'),
        ('15', 'GM MF P2I alias', 'Music', 'GM MF P2I'),
        ('16', 'GM MF P2J alias', 'Music', 'GM MF P2J'),
        ('17', 'GM MF P2K alias', 'Music', 'GM MF P2K'),
        ('18', 'GM MF P2L alias', 'Music', 'GM MF P2L'),
        ('19', 'BW', 'Halls', 'BW'),
        ('20', 'CASINO', 'Casino', 'CASINO')
    ]
    member_tiers = ['Basic', 'Silver', 'Gold', 'Diamond']
    start_date = datetime.strptime(start_date, "%Y-%m-%d")

    # 初始化700个用户的基础属性
    users = []
    for uid in range(1, num_users + 1):
        profile_type = random.choice([1, 2, 3, 4])
        member_tier = random.choice(member_tiers) if profile_type == 1 else ''
        age = random.randint(18, 70)
        gender = random.choice([1, 2])
        users.append((uid, profile_type, member_tier, age, gender))

    # 全局计数器：控制总记录数不超过300万
    total_generated = 0
    # 单用户平均记录数（300万/700≈4285），小幅浮动避免超标
    avg_records_per_user = total_records // num_users

    for uid, profile_type, member_tier, age, gender in users:
        if total_generated >= total_records:
            break  # 达到总记录数，停止生成

        # 单用户最大记录数（平均±100浮动）
        user_max_records = avg_records_per_user + random.randint(-100, 100)
        user_generated = 0

        for day_offset in range(days):
            if total_generated >= total_records or user_generated >= user_max_records:
                break

            day_start = start_date + timedelta(days=day_offset)
            day_end = day_start + timedelta(days=1) - timedelta(seconds=1)

            # 核心修改：遍历全天0-23小时，确保每个小时都有记录
            for hour in range(24):
                if total_generated >= total_records or user_generated >= user_max_records:
                    break

                # 每小时随机选1个区域（控制数据量，避免暴增）
                region = random.choice(regions)
                region_id, region_name, sector, region_name_heatmap = region
                camera_id = int(f"{region_id}{random.randint(1, 5)}")

                # 初始化该小时的起始时间（小时内随机分钟/秒，覆盖整小时）
                hour_start_time = day_start + timedelta(
                    hours=hour,
                    minutes=random.randint(0, 5),  # 小时内前5分钟开始，留足停留时间
                    seconds=random.randint(0, 59)
                )
                hour_end_time = hour_start_time + timedelta(hours=1)  # 当前小时的结束时间
                # 防止停留时间跨小时/跨天
                hour_end_time = min(hour_end_time, day_end)

                # 核心规则：每小时每区域停留3-20分钟
                stay_minutes = random.randint(3, 20)
                stay_seconds = stay_minutes * 60
                stay_end_time = hour_start_time + timedelta(seconds=stay_seconds)
                stay_end_time = min(stay_end_time, hour_end_time)  # 严格限制在当前小时内

                current_time = hour_start_time
                # 按20-50秒间隔生成记录（平衡粒度和数据量）
                while current_time <= stay_end_time and total_generated < total_records and user_generated < user_max_records:
                    yield (
                        uid, profile_type, member_tier, age, gender,
                        camera_id, region_id, region_name, sector, current_time, region_name_heatmap, datetime.now()
                    )
                    # 更新计数器
                    total_generated += 1
                    user_generated += 1
                    # 每条记录间隔20-50秒，控制每小时记录数
                    current_time += timedelta(seconds=random.randint(20, 50))

    print(f"数据生成完成，实际生成总记录数：{total_generated}")


# ---------------------
# 批量写入（适配300万数据量）
# ---------------------
def insert_in_batches(client, table_name, generator, batch_size=20000):
    insert_sql = f"""
    INSERT INTO {table_name} 
    (profile_id, profile_type, member_tier, age, gender, camera_id, region_id, region_name,sector, capture_time,region_name_heatmap,batch_time)
    VALUES
    """
    batch = []
    total_written = 0
    for record in generator:
        batch.append(record)
        if len(batch) >= batch_size:
            client.execute(insert_sql, batch)
            total_written += len(batch)
            print(f"已写入 {total_written} 条 / 目标3000000条")
            batch = []
    # 处理最后一批数据
    if batch:
        client.execute(insert_sql, batch)
        total_written += len(batch)
        print(f"最终写入总记录数：{total_written} 条 (完成)")


# ---------------------
# 主流程：700用户/31天/300万条/全天0-23小时有记录
# ---------------------
if __name__ == "__main__":
    gen = mock_data_generator(
        num_users=700,
        total_records=3000000,
        start_date="2025-08-01",
        days=31
    )
    insert_in_batches(client, "dwd_user_capture_original", gen, batch_size=20000)