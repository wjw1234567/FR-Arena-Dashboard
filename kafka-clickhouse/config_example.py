# 配置文件示例 - 展示所有可配置参数

# ============================================
# Kafka配置
# ============================================
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']  # Kafka服务器地址
KAFKA_TOPIC = 'data_stream'                    # Kafka主题名称

# ============================================
# ClickHouse配置
# ============================================
CLICKHOUSE_HOST = 'localhost'                  # ClickHouse主机地址
CLICKHOUSE_PORT = 9000                         # ClickHouse端口（Native协议）
CLICKHOUSE_DATABASE = 'default'                # 数据库名称
CLICKHOUSE_USER = 'default'                    # 用户名
CLICKHOUSE_PASSWORD = ''                       # 密码（默认无密码）
CLICKHOUSE_TABLE = 'stream_data'               # 表名

# 如需设置密码，示例：
# CLICKHOUSE_PASSWORD = 'your_secure_password'

# ============================================
# 数据生成配置（生产者端）
# ============================================
DATA_RATE_MIN = 10                             # 每秒最少生成数据条数
DATA_RATE_MAX = 15                             # 每秒最多生成数据条数
BATCH_INTERVAL = 1.0                           # 批次间隔（秒）

# ============================================
# 消费者配置
# ============================================
CONSUMER_BATCH_SIZE = 50                       # 批量写入ClickHouse的大小

# ============================================
# 其他配置
# ============================================
ID_COUNTER_FILE = 'last_id.txt'                # ID持久化文件（用于程序重启后续增）
