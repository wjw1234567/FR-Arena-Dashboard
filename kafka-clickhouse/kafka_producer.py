"""
Kafka生产者模块
"""
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
import config


class KafkaDataProducer:
    def __init__(self):
        """初始化Kafka生产者"""
        self.producer = None
        self._connect()
    
    def _connect(self):
        """连接到Kafka集群"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                # 确保消息可靠性的配置
                acks='all',  # 等待所有副本确认
                retries=3,   # 失败重试次数
                max_in_flight_requests_per_connection=1  # 保证消息顺序
            )
            print(f"[Kafka生产者] 成功连接到 {config.KAFKA_BOOTSTRAP_SERVERS}")
            print(f"[Kafka生产者] 目标Topic: {config.KAFKA_TOPIC}")
        except Exception as e:
            print(f"[错误] Kafka连接失败: {e}")
            raise
    
    def send_batch(self, batch: list) -> bool:
        """
        批量发送数据到Kafka
        参数: batch - 数据列表
        返回: 是否全部发送成功
        """
        if not self.producer:
            print("[错误] Kafka生产者未初始化")
            return False
        
        success_count = 0
        fail_count = 0
        
        for record in batch:
            try:
                # 异步发送消息
                future = self.producer.send(config.KAFKA_TOPIC, value=record)
                
                # 等待发送结果（可选，确保可靠性）
                record_metadata = future.get(timeout=10)
                success_count += 1
                
            except KafkaError as e:
                print(f"[错误] 消息发送失败: {e}, 数据: {record}")
                fail_count += 1
            except Exception as e:
                print(f"[错误] 未知错误: {e}")
                fail_count += 1
        
        # 确保所有消息都已发送
        self.producer.flush()
        
        if fail_count > 0:
            print(f"[Kafka生产者] 发送完成: 成功 {success_count}, 失败 {fail_count}")
        
        return fail_count == 0
    
    def close(self):
        """关闭Kafka生产者"""
        if self.producer:
            self.producer.close()
            print("[Kafka生产者] 已关闭连接")


if __name__ == "__main__":
    # 测试Kafka生产者
    producer = KafkaDataProducer()
    test_data = [
        {"date_time": "2024-01-01 12:00:00", "ID": 1},
        {"date_time": "2024-01-01 12:00:01", "ID": 2}
    ]
    producer.send_batch(test_data)
    producer.close()
