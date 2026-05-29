"""
数据生成模块 - 每秒生成10-15条结构化数据
"""
import random
import time
from datetime import datetime
from typing import Dict, Generator
import os
import config


class DataGenerator:
    def __init__(self):
        """初始化数据生成器，加载上次的ID计数"""
        self.current_id = self._load_last_id()
        print(f"[数据生成器] 初始化完成，起始ID: {self.current_id}")
    
    def _load_last_id(self) -> int:
        """从文件加载上次的ID，实现续增功能"""
        if os.path.exists(config.ID_COUNTER_FILE):
            try:
                with open(config.ID_COUNTER_FILE, 'r') as f:
                    last_id = int(f.read().strip())
                    return last_id + 1
            except Exception as e:
                print(f"[警告] 读取ID文件失败: {e}，从1开始")
                return 1
        return 1
    
    def _save_last_id(self):
        """保存当前ID到文件"""
        try:
            with open(config.ID_COUNTER_FILE, 'w') as f:
                f.write(str(self.current_id))
        except Exception as e:
            print(f"[警告] 保存ID文件失败: {e}")
    
    def generate_record(self) -> Dict:
        """
        生成单条数据记录
        返回格式: {"date_time": "YYYY-MM-DD hh:mm:ss", "ID": int}
        """
        record = {
            "date_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ID": self.current_id
        }
        self.current_id += 1
        return record
    
    def generate_batch(self) -> list:
        """
        生成一批数据（10-15条）
        """
        count = random.randint(config.DATA_RATE_MIN, config.DATA_RATE_MAX)
        batch = [self.generate_record() for _ in range(count)]
        return batch
    
    def run(self) -> Generator[list, None, None]:
        """
        持续生成数据的生成器
        每秒生成一批数据并yield
        """
        print(f"[数据生成器] 开始运行，每秒生成 {config.DATA_RATE_MIN}-{config.DATA_RATE_MAX} 条数据")
        try:
            while True:
                start_time = time.time()
                
                # 生成一批数据
                batch = self.generate_batch()
                yield batch
                
                # 定期保存ID（每100条保存一次）
                if self.current_id % 100 == 0:
                    self._save_last_id()
                
                # 控制生成速率
                elapsed = time.time() - start_time
                sleep_time = max(0, config.BATCH_INTERVAL - elapsed)
                time.sleep(sleep_time)
        
        except KeyboardInterrupt:
            print("\n[数据生成器] 收到停止信号")
        finally:
            # 程序退出时保存最后的ID
            self._save_last_id()
            print(f"[数据生成器] 已保存最后ID: {self.current_id}")


if __name__ == "__main__":
    # 测试数据生成器
    generator = DataGenerator()
    for batch in generator.run():
        print(f"生成 {len(batch)} 条数据: {batch[:2]}...")  # 只打印前2条
