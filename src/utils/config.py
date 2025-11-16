import os
from dotenv import load_dotenv
import logging

# 加载环境变量
load_dotenv()

# 直接使用logging模块
import logging
logger = logging.getLogger(__name__)

class Config:
    """项目配置类"""
    
    # GitHub API配置
    GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
    GITHUB_API_RATE_LIMIT_WAIT = int(os.getenv('GITHUB_API_RATE_LIMIT_WAIT', '60'))
    
    # MySQL数据库配置
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = int(os.getenv('DB_PORT', '3306'))
    DB_USER = os.getenv('DB_USER', 'admin')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'password')
    DB_NAME = os.getenv('DB_NAME', 'example_db')
    
    # 项目配置
    OUTPUT_DIR = os.getenv('OUTPUT_DIR', 'output')
    MAX_PROJECTS = int(os.getenv('MAX_PROJECTS', '5000'))
    
    # 项目筛选条件
    MIN_STARS = 1000
    MIN_FORKS = 100
    MIN_COMMITS = 50
    START_DATE = '2025-01-01T00:00:00Z'  # 2025年以来
    
    # 数据库连接字符串
    @property
    def DB_CONNECTION_STRING(self):
        return {
            'host': self.DB_HOST,
            'user': self.DB_USER,
            'password': self.DB_PASSWORD,
            'database': self.DB_NAME,
            'port': self.DB_PORT
        }
    
    @staticmethod
    def ensure_output_directory():
        """确保输出目录存在"""
        if not os.path.exists(Config.OUTPUT_DIR):
            os.makedirs(Config.OUTPUT_DIR)
            logger.info(f"创建输出目录: {Config.OUTPUT_DIR}")

# 创建全局配置实例
config = Config()