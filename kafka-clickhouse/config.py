# 配置文件 - 所有可调整参数集中管理

# Kafka配置
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'data_stream'  # 可自定义Topic名称

# ClickHouse配置
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 9000
CLICKHOUSE_DATABASE = 'Facial'
CLICKHOUSE_USER = 'default'
CLICKHOUSE_PASSWORD = 'ck_test'  # 默认无密码，如需密码请修改
CLICKHOUSE_TABLE = 'stream_data'

# 数据生成配置（生产者端）
DATA_RATE_MIN = 10  # 每秒最少生成数据条数
DATA_RATE_MAX = 15  # 每秒最多生成数据条数
BATCH_INTERVAL = 1.0  # 批次间隔（秒）

# 消费者配置
CONSUMER_BATCH_SIZE = 50  # 批量写入ClickHouse的大小

# ID持久化文件（用于程序重启后续增）
ID_COUNTER_FILE = 'last_id.txt'
