"""
主程序 - 数据生产者
运行方式: python main.py
说明: 生成数据并发送到Kafka，需同时运行 kafka_consumer.py
"""
import time
import signal
from data_generator import DataGenerator
from kafka_producer import KafkaDataProducer
import config


class DataProducerPipeline:
    def __init__(self):
        """初始化数据生产管道"""
        self.running = True
        self.generator = None
        self.kafka_producer = None
        
        # 注册信号处理器（优雅退出）
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """处理退出信号"""
        print("\n[主程序] 收到退出信号，正在关闭...")
        self.running = False
    
    def initialize(self):
        """初始化所有组件"""
        print("=" * 60)
        print("数据生产者启动中...")
        print("=" * 60)
        
        try:
            # 初始化数据生成器
            self.generator = DataGenerator()
            
            # 初始化Kafka生产者
            self.kafka_producer = KafkaDataProducer()
            
            print("=" * 60)
            print("数据生产者初始化成功！")
            print(f"配置: 每秒生成 {config.DATA_RATE_MIN}-{config.DATA_RATE_MAX} 条数据")
            print(f"Kafka Topic: {config.KAFKA_TOPIC}")
            print("=" * 60)
            print("\n⚠️  提示: 请确保同时运行 kafka_consumer.py")
            print("   命令: python kafka_consumer.py\n")
            return True
            
        except Exception as e:
            print(f"[错误] 初始化失败: {e}")
            return False
    
    def run(self):
        """运行数据生产管道"""
        if not self.initialize():
            print("[错误] 初始化失败，程序退出")
            return
        
        print("开始生成数据并发送到Kafka... (按 Ctrl+C 停止)\n")
        
        total_generated = 0
        total_sent = 0
        start_time = time.time()
        last_print_time = start_time
        
        try:
            for batch in self.generator.run():
                if not self.running:
                    break
                
                batch_size = len(batch)
                total_generated += batch_size
                
                # 发送到Kafka
                kafka_success = self.kafka_producer.send_batch(batch)
                if kafka_success:
                    total_sent += batch_size
                
                # 每10秒打印一次统计信息
                current_time = time.time()
                if current_time - last_print_time >= 10:
                    elapsed = current_time - start_time
                    avg_rate = total_generated / elapsed
                    print(f"\n[统计] 运行时长: {int(elapsed)}秒")
                    print(f"  已生成: {total_generated} 条")
                    print(f"  已发送到Kafka: {total_sent} 条")
                    print(f"  平均速率: {avg_rate:.1f} 条/秒\n")
                    last_print_time = current_time
        
        except Exception as e:
            print(f"\n[错误] 运行时错误: {e}")
        
        finally:
            self.cleanup()
            
            # 最终统计
            elapsed = time.time() - start_time
            print("\n" + "=" * 60)
            print("数据生产者已停止")
            print("=" * 60)
            print(f"总运行时长: {elapsed:.1f} 秒")
            print(f"总生成数据: {total_generated} 条")
            print(f"总发送到Kafka: {total_sent} 条")
            print("=" * 60)
    
    def cleanup(self):
        """清理资源"""
        print("\n[主程序] 正在清理资源...")
        
        if self.kafka_producer:
            self.kafka_producer.close()
        
        print("[主程序] 资源清理完成")


if __name__ == "__main__":
    pipeline = DataProducerPipeline()
    pipeline.run()
