"""
创建Kafka Topic的辅助脚本
运行方式: python create_kafka_topic.py
"""
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import config


def create_topic():
    """创建Kafka Topic"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            client_id='topic_creator'
        )
        
        topic = NewTopic(
            name=config.KAFKA_TOPIC,
            num_partitions=1,
            replication_factor=1
        )
        
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"✓ Topic '{config.KAFKA_TOPIC}' 创建成功")
        
        admin_client.close()
        
    except TopicAlreadyExistsError:
        print(f"✓ Topic '{config.KAFKA_TOPIC}' 已存在")
    except Exception as e:
        print(f"✗ 创建Topic失败: {e}")


if __name__ == "__main__":
    create_topic()
