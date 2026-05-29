"""
Arena Capture Data Generator
生成符合业务逻辑的模拟人员轨迹明细数据，并直接写入ClickHouse
"""

import pandas as pd
import random
from datetime import datetime, timedelta
import uuid
from clickhouse_driver import Client


class ArenaCaptureDataGenerator:
    """人员轨迹数据生成器"""
    
    def __init__(self, camera_csv='dim_arena_camera.csv', event_csv='dim_event_list.csv'):
        """
        初始化数据生成器
        
        Args:
            camera_csv: 摄像头维度表CSV文件路径
            event_csv: 事件维度表CSV文件路径
        """
        # 加载维度表
        self.camera_df = pd.read_csv(camera_csv)
        self.event_df = pd.read_csv(event_csv)
        
        # 解析事件表的日期时间字段
        self.event_df['CDate'] = pd.to_datetime(self.event_df['CDate'])
        self.event_df['StartDtm'] = pd.to_datetime(self.event_df['StartDtm'])
        self.event_df['EndDtm'] = pd.to_datetime(self.event_df['EndDtm'])
        
        # 获取合法的camera_id列表
        self.valid_camera_ids = self.camera_df['camera_id'].tolist()
        
        # 会员等级枚举
        self.member_tiers = ["Diamond", "Gold", "Basic", "Silver"]
        
        # 性别枚举 (1=男性, 2=女性)
        self.genders = [1, 2]
        
    def generate_capture_time(self, event_row, is_concert=False):
        """
        生成抓拍时间
        
        Args:
            event_row: 事件行数据
            is_concert: 是否为演唱会类型
            
        Returns:
            datetime: 生成的抓拍时间
        """
        start_time = event_row['StartDtm']
        end_time = event_row['EndDtm']
        
        if is_concert:
            # 演唱会：前4小时到后1小时都可能有人员出现
            early_arrival_start = start_time - timedelta(hours=4)  # 提前4小时开始
            peak_before_start = start_time - timedelta(hours=1)    # 前1小时高峰
            peak_after_end = end_time + timedelta(hours=1)         # 后1小时高峰
            
            # 时间段分布概率：
            # 20% 在开始前4-2小时（提前到达）
            # 50% 在开始前1小时（高峰期）
            # 20% 在活动期间
            # 10% 在结束后1小时（离场）
            rand = random.random()
            
            if rand < 0.2:
                # 提前到达：开始前4-2小时
                early_end = start_time - timedelta(hours=2)
                time_range = (early_end - early_arrival_start).total_seconds()
                random_seconds = random.uniform(0, time_range)
                return early_arrival_start + timedelta(seconds=random_seconds)
            elif rand < 0.7:
                # 高峰期：开始前1小时
                time_range = (start_time - peak_before_start).total_seconds()
                random_seconds = random.uniform(0, time_range)
                return peak_before_start + timedelta(seconds=random_seconds)
            elif rand < 0.9:
                # 活动期间
                time_range = (end_time - start_time).total_seconds()
                random_seconds = random.uniform(0, time_range)
                return start_time + timedelta(seconds=random_seconds)
            else:
                # 离场：结束后1小时
                time_range = (peak_after_end - end_time).total_seconds()
                random_seconds = random.uniform(0, time_range)
                return end_time + timedelta(seconds=random_seconds)
        else:
            # 非演唱会：均匀分布在活动时间内
            time_range = (end_time - start_time).total_seconds()
            random_seconds = random.uniform(0, time_range)
            return start_time + timedelta(seconds=random_seconds)
    
    def generate_person_trajectory(self, event_row, profile_id, profile_type, member_tier, age, gender, num_captures):
        """
        为单个人员生成轨迹记录
        
        Args:
            event_row: 事件行数据
            profile_id: 人员ID
            profile_type: 人员类型（1/2/3）
            member_tier: 会员等级（仅profile_type=1时有值）
            age: 年龄
            gender: 性别
            num_captures: 该人员的抓拍次数
            
        Returns:
            list: 轨迹记录列表
        """
        # 判断是否为演唱会
        is_concert = event_row['EventType'] == 'concert'
        
        trajectories = []
        used_cameras = set()
        
        for _ in range(num_captures):
            # 随机选择摄像头（避免连续重复）
            available_cameras = [c for c in self.valid_camera_ids if c not in used_cameras or len(used_cameras) >= len(self.valid_camera_ids) * 0.8]
            if not available_cameras:
                available_cameras = self.valid_camera_ids
                used_cameras.clear()
            
            camera_id = random.choice(available_cameras)
            used_cameras.add(camera_id)
            
            # 生成抓拍时间
            capture_time = self.generate_capture_time(event_row, is_concert)
            
            # 生成记录
            record = {
                'profile_id': profile_id,
                'person_id': f'P{profile_id}',
                'profile_type': profile_type,
                'member_tier': member_tier,  # 只有profile_type=1时才有值
                'member_id': f'M{profile_id:010d}' if profile_type == 1 else None,
                'is_delete': 0,
                'person_status': 1,
                'album_id': f'ALB{random.randint(1000, 9999)}',
                'merge_count': random.randint(0, 5),
                'face_count': random.randint(1, 10),
                'identify_num': f'ID{random.randint(100000, 999999)}',
                'card_type': random.choice(['ID', 'Passport', 'Driver License', None]),
                'address': None,
                'name': f'Person_{profile_id}',
                'age': age,
                'gender': gender,
                'capture_id': str(uuid.uuid4()),
                'camera_id': camera_id,
                'capture_time': capture_time,
                'batch_time': datetime.now()
            }
            
            trajectories.append(record)
        
        # 按时间排序
        trajectories.sort(key=lambda x: x['capture_time'])
        
        return trajectories
    
    def generate_data(self, num_persons_per_event=100, min_captures=2, max_captures=8, 
                     event_filter=None, output_csv=None):
        """
        批量生成数据
        
        Args:
            num_persons_per_event: 每个事件生成的人员数量
            min_captures: 每个人员最少抓拍次数
            max_captures: 每个人员最多抓拍次数
            event_filter: 事件过滤函数（可选）
            output_csv: 输出CSV文件路径（可选，如果提供则同时保存CSV）
            
        Returns:
            pd.DataFrame: 生成的数据
        """
        all_records = []
        profile_id_counter = 1000000  # 起始profile_id
        
        # 用于存储每个profile_id的固定属性
        person_profiles = {}
        
        # 筛选事件
        events = self.event_df
        if event_filter:
            events = events[events.apply(event_filter, axis=1)]
        
        print(f"开始生成数据，共 {len(events)} 个事件...")
        
        for idx, event_row in events.iterrows():
            event_title = event_row['Title']
            event_date = event_row['CDate'].strftime('%Y-%m-%d')
            
            print(f"处理事件 [{idx+1}/{len(events)}]: {event_title} ({event_date})")
            
            # 为该事件生成多个人员的轨迹
            for _ in range(num_persons_per_event):
                profile_id = profile_id_counter
                profile_id_counter += 1
                
                # 为该人员生成固定属性（profile_id与这些属性一对一关系）
                profile_type = random.choice([1, 2, 3])
                
                # 只有profile_type=1时才有member_tier
                if profile_type == 1:
                    member_tier = random.choice(self.member_tiers)
                else:
                    member_tier = None
                
                age = random.randint(21, 65)
                gender = random.choice(self.genders)
                
                # 存储该人员的固定属性
                person_profiles[profile_id] = {
                    'profile_type': profile_type,
                    'member_tier': member_tier,
                    'age': age,
                    'gender': gender
                }
                
                # 随机生成该人员的抓拍次数
                num_captures = random.randint(min_captures, max_captures)
                
                # 生成轨迹
                trajectories = self.generate_person_trajectory(
                    event_row, profile_id, profile_type, member_tier, age, gender, num_captures
                )
                all_records.extend(trajectories)
        
        # 转换为DataFrame
        df = pd.DataFrame(all_records)
        
        print(f"\n数据生成完成！")
        print(f"总记录数: {len(df)}")
        print(f"总人员数: {df['profile_id'].nunique()}")
        print(f"profile_type=1的人员数: {sum(1 for p in person_profiles.values() if p['profile_type'] == 1)}")
        print(f"profile_type=2的人员数: {sum(1 for p in person_profiles.values() if p['profile_type'] == 2)}")
        print(f"profile_type=3的人员数: {sum(1 for p in person_profiles.values() if p['profile_type'] == 3)}")
        
        # 如果指定了CSV输出，保存到文件
        if output_csv:
            df_csv = df.copy()
            df_csv['capture_time'] = df_csv['capture_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
            df_csv['batch_time'] = df_csv['batch_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
            df_csv.to_csv(output_csv, index=False, encoding='utf-8-sig')
            print(f"CSV文件已保存: {output_csv}")
        
        return df
    
    def insert_to_clickhouse(self, df, host='localhost', port=9000, database='Facial', 
                            table='dwd_arena_capture_original', user='default', password='',
                            batch_size=1000):
        """
        将数据插入ClickHouse
        
        Args:
            df: 要插入的DataFrame
            host: ClickHouse主机地址
            port: ClickHouse端口
            database: 数据库名
            table: 表名
            user: 用户名
            password: 密码
            batch_size: 批量插入大小
            
        Returns:
            bool: 是否成功
        """
        try:
            # 连接ClickHouse
            print(f"\n连接ClickHouse: {host}:{port}/{database}")
            client = Client(host=host, port=port, user=user, password=password, database=database)
            
            # 测试连接
            result = client.execute('SELECT 1')
            print("ClickHouse连接成功！")
            
            # 准备插入数据
            total_rows = len(df)
            print(f"\n开始插入数据到表 {database}.{table}，共 {total_rows} 条记录...")
            
            # 按字段顺序准备数据
            columns = [
                'profile_id', 'person_id', 'profile_type', 'member_tier', 'member_id',
                'is_delete', 'person_status', 'album_id', 'merge_count', 'face_count',
                'identify_num', 'card_type', 'address', 'name', 'age', 'gender',
                'capture_id', 'camera_id', 'capture_time', 'batch_time'
            ]
            
            # 分批插入
            inserted_count = 0
            for start_idx in range(0, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx]
                
                # 准备批量数据
                batch_data = []
                for _, row in batch_df.iterrows():
                    batch_data.append([row[col] for col in columns])
                
                # 插入数据
                client.execute(
                    f'INSERT INTO {database}.{table} VALUES',
                    batch_data
                )
                
                inserted_count += len(batch_data)
                print(f"已插入: {inserted_count}/{total_rows} ({inserted_count*100//total_rows}%)")
            
            print(f"\n✓ 数据插入完成！共插入 {inserted_count} 条记录")
            
            # 验证插入
            count_result = client.execute(f'SELECT count() FROM {database}.{table}')
            print(f"表中当前总记录数: {count_result[0][0]}")
            
            return True
            
        except Exception as e:
            print(f"\n✗ 插入ClickHouse失败: {str(e)}")
            return False


def main():
    """主函数 - 示例用法"""
    
    # ClickHouse连接配置
    CH_CONFIG = {
        'host': 'localhost',      # 修改为你的ClickHouse地址
        'port': 9000,             # 默认端口
        'database': 'Facial',     # 数据库名
        'table': 'dwd_arena_capture_original',
        'user': 'default',        # 用户名
        'password': 'ck_test'            # 密码
    }
    
    # 创建生成器
    generator = ArenaCaptureDataGenerator(
        camera_csv='dim_arena_camera.csv',
        event_csv='dim_event_list.csv'
    )
    
    # 示例1: 生成所有事件的数据并写入ClickHouse
    # df = generator.generate_data(
    #     num_persons_per_event=100,
    #     min_captures=2,
    #     max_captures=8,
    #     output_csv='arena_capture_data_all.csv'  # 可选：同时保存CSV备份
    # )
    
    # 示例2: 只生成2026年的演唱会数据
    def filter_2026_concerts(row):
        return row['CDate'].year == 2026 and row['EventType'] == 'concert'
    
    # 生成数据
    df = generator.generate_data(
        num_persons_per_event=1500,  # 每场演唱会150人
        min_captures=10,
        max_captures=53,
        event_filter=filter_2026_concerts,
        output_csv=None  # 设置为文件名可同时保存CSV备份
    )
    
    # 显示数据统计
    print("\n=== 数据统计 ===")
    print(f"会员等级分布:\n{df['member_tier'].value_counts()}")
    print(f"\n性别分布:\n{df['gender'].value_counts()}")
    print(f"\n年龄范围: {df['age'].min()} - {df['age'].max()}")
    print(f"\n涉及摄像头数量: {df['camera_id'].nunique()}")
    
    # 插入到ClickHouse
    success = generator.insert_to_clickhouse(
        df=df,
        host=CH_CONFIG['host'],
        port=CH_CONFIG['port'],
        database=CH_CONFIG['database'],
        table=CH_CONFIG['table'],
        user=CH_CONFIG['user'],
        password=CH_CONFIG['password'],
        batch_size=10000
    )
    
    if success:
        print("\n✓ 所有操作完成！")
    else:
        print("\n✗ 插入失败，请检查ClickHouse连接配置")


if __name__ == '__main__':
    main()
