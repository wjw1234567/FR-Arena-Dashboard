# Arena人员轨迹数据生成器

## 功能说明
批量生成符合业务逻辑的模拟人员轨迹数据，并直接写入ClickHouse数据库的 `Facial.dwd_arena_capture_original` 表。

## 安装依赖

```bash
pip install -r requirements.txt
```

或手动安装：
```bash
pip install pandas clickhouse-driver
```

## 配置ClickHouse连接

编辑 `generate_arena_capture_data.py` 中的 `CH_CONFIG` 配置：

```python
CH_CONFIG = {
    'host': 'localhost',      # 修改为你的ClickHouse地址
    'port': 9000,             # 默认端口
    'database': 'Facial',     # 数据库名
    'table': 'dwd_arena_capture_original',
    'user': 'default',        # 用户名
    'password': ''            # 密码（如有）
}
```

## 使用方法

### 基本用法
```bash
python generate_arena_capture_data.py
```

### 自定义参数

在 `main()` 函数中修改参数：

```python
df = generator.generate_data(
    num_persons_per_event=150,  # 每个事件生成的人员数量
    min_captures=3,             # 每人最少抓拍次数
    max_captures=10,            # 每人最多抓拍次数
    event_filter=filter_func,   # 事件过滤函数（可选）
    output_csv='backup.csv'     # 同时保存CSV备份（可选）
)
```

### 事件过滤示例

```python
# 只生成2026年的演唱会数据
def filter_2026_concerts(row):
    return row['CDate'].year == 2026 and row['EventType'] == 'concert'

# 只生成特定日期范围的数据
def filter_date_range(row):
    return row['CDate'] >= pd.Timestamp('2026-01-01') and row['CDate'] <= pd.Timestamp('2026-03-31')

# 生成所有事件数据（不使用过滤器）
df = generator.generate_data(event_filter=None)
```

## 数据生成规则

- **profile_id**: 唯一人员ID，从1000000开始递增
- **profile_type**: 随机分配 [1, 2, 3]
- **member_tier**: 
  - 仅当 profile_type=1 时有值，随机分配 ["Diamond", "Gold", "Basic", "Silver"]
  - profile_type=2 或 3 时为 NULL
- **member_id**: 仅当 profile_type=1 时有值
- **age**: 21-65岁随机整数
- **gender**: 1=男性，2=女性
- **一对一关系**: profile_id 与 profile_type、member_tier、age、gender 是一对一关系（同一个人的所有轨迹记录这些字段保持一致）
- **camera_id**: 从 dim_arena_camera 表中随机选择
- **capture_time**: 
  - 演唱会：70%在开始前1小时或结束后1小时（高峰期）
  - 其他事件：均匀分布在活动时间内
- **轨迹逻辑**: 单个人员在同一天经过多个摄像头区域

## 输出示例

```
开始生成数据，共 15 个事件...
处理事件 [1/15]: Taiwanese Male Singer (2026-06-08)
处理事件 [2/15]: EXO Concert (2026-05-24)
...

数据生成完成！
总记录数: 9750
总人员数: 1500
profile_type=1的人员数: 502
profile_type=2的人员数: 498
profile_type=3的人员数: 500

连接ClickHouse: localhost:9000/Facial
ClickHouse连接成功！

开始插入数据到表 Facial.dwd_arena_capture_original，共 9750 条记录...
已插入: 1000/9750 (10%)
已插入: 2000/9750 (20%)
...
已插入: 9750/9750 (100%)

✓ 数据插入完成！共插入 9750 条记录
表中当前总记录数: 9750

✓ 所有操作完成！
```

## 注意事项

1. 确保ClickHouse服务正在运行
2. 确保 `Facial.dwd_arena_capture_original` 表已创建
3. 确保用户有插入权限
4. 首次运行建议先用小数据量测试（如 num_persons_per_event=10）
