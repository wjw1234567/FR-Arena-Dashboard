"""
Kafka消费者 - 从Kafka消费数据并写入ClickHouse
运行方式: python kafka_consumer.py
"""
import json
import time
import signal
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from clickhouse_writer import ClickHouseWriter
import config


class KafkaDataConsumer:
    def __init__(self):
        """初始化Kafka消费者"""
        self.consumer = None
        self.clickhouse_writer = None
        self.running = True
        
        # 注册信号处理器（优雅退出）
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """处理退出信号"""
        print("\n[Kafka消费者] 收到退出信号，正在关闭...")
        self.running = False
    
    def _connect(self):
        """连接到Kafka集群"""
        try:
            self.consumer = KafkaConsumer(
                config.KAFKA_TOPIC,
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',  # 从最早的消息开始消费
                enable_auto_commit=True,
                group_id='clickhouse_writer_group',
                max_poll_records=500  # 每次拉取最多500条消息
            )
            print(f"[Kafka消费者] 成功连接到 {config.KAFKA_BOOTSTRAP_SERVERS}")
            print(f"[Kafka消费者] 订阅Topic: {config.KAFKA_TOPIC}")
        except Exception as e:
            print(f"[错误] Kafka消费者连接失败: {e}")
            raise
    
    def consume_and_write(self):
        """持续消费Kafka消息并写入ClickHouse"""
        print("=" * 60)
        print("Kafka消费者启动中...")
        print("=" * 60)
        
        try:
            # 连接Kafka
            self._connect()
            
            # 初始化ClickHouse写入器
            self.clickhouse_writer = ClickHouseWriter()
            
            print("=" * 60)
            print("消费者初始化成功！")
            print(f"Kafka Topic: {config.KAFKA_TOPIC}")
            print(f"ClickHouse表: {config.CLICKHOUSE_TABLE}")
            print(f"批量写入大小: {config.CONSUMER_BATCH_SIZE}")
            print("=" * 60)
            
        except Exception as e:
            print(f"[错误] 初始化失败: {e}")
            return
        
        print("\n开始消费Kafka消息并写入ClickHouse... (按 Ctrl+C 停止)\n")
        
        batch = []
        total_consumed = 0
        total_written = 0
        start_time = time.time()
        last_print_time = start_time
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                
                try:
                    record = message.value
                    batch.append(record)
                    total_consumed += 1
                    
                    # 达到批量大小时写入
                    if len(batch) >= config.CONSUMER_BATCH_SIZE:
                        success = self.clickhouse_writer.write_batch(batch)
                        if success:
                            total_written += len(batch)
                        batch = []
                    
                    # 每10秒打印一次统计信息
                    current_time = time.time()
                    if current_time - last_print_time >= 10:
                        elapsed = current_time - start_time
                        avg_rate = total_consumed / elapsed
                        
                        # 查询ClickHouse中的总记录数
                        db_count = self.clickhouse_writer.query_count()
                        
                        print(f"\n[统计] 运行时长: {int(elapsed)}秒")
                        print(f"  已消费: {total_consumed} 条")
                        print(f"  已写入ClickHouse: {total_written} 条")
                        print(f"  平均速率: {avg_rate:.1f} 条/秒")
                        print(f"  ClickHouse总记录数: {db_count}\n")
                        last_print_time = current_time
                
                except json.JSONDecodeError as e:
                    print(f"[错误] JSON解析失败: {e}, 消息: {message.value}")
                except KeyError as e:
                    print(f"[错误] 数据格式错误，缺少字段: {e}, 消息: {message.value}")
                except Exception as e:
                    print(f"[错误] 处理消息失败: {e}, 消息: {message.value}")
        
        except Exception as e:
            print(f"\n[错误] 运行时错误: {e}")
        
        finally:
            # 写入剩余数据
            if batch:
                success = self.clickhouse_writer.write_batch(batch)
                if success:
                    total_written += len(batch)
                print(f"[Kafka消费者] 写入剩余 {len(batch)} 条数据")
            
            self.close()
            
            # 最终统计
            elapsed = time.time() - start_time
            print("\n" + "=" * 60)
            print("Kafka消费者已停止")
            print("=" * 60)
            print(f"总运行时长: {elapsed:.1f} 秒")
            print(f"总消费消息: {total_consumed} 条")
            print(f"总写入ClickHouse: {total_written} 条")
            print("=" * 60)
    
    def close(self):
        """关闭连接"""
        print("\n[Kafka消费者] 正在清理资源...")
        
        if self.consumer:
            self.consumer.close()
            print("[Kafka消费者] 已关闭Kafka连接")
        
        if self.clickhouse_writer:
            self.clickhouse_writer.close()
        
        print("[Kafka消费者] 资源清理完成")


if __name__ == "__main__":
    consumer = KafkaDataConsumer()
    consumer.consume_and_write()
