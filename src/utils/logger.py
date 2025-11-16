import logging
import os
from datetime import datetime

# 避免循环导入，在需要时再导入config

class LoggerManager:
    """日志管理类"""
    
    @staticmethod
    def get_logger(name, log_file=None):
        """获取日志记录器
        
        Args:
            name: 日志记录器名称
            log_file: 日志文件路径，如果为None则只输出到控制台
            
        Returns:
            logging.Logger: 配置好的日志记录器
        """
        logger = logging.getLogger(name)
        
        # 避免重复添加处理器
        if logger.handlers:
            return logger
        
        # 延迟导入config以避免循环导入
        from .config import config
        
        # 设置日志级别
        log_level = getattr(logging, getattr(config, 'LOG_LEVEL', 'INFO'), logging.INFO)
        logger.setLevel(log_level)
        
        # 创建格式器
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # 创建文件处理器（如果指定了日志文件）
        if log_file:
            # 确保输出目录存在
            if not os.path.exists(config.OUTPUT_DIR):
                os.makedirs(config.OUTPUT_DIR)
                logging.info(f"创建输出目录: {config.OUTPUT_DIR}")
            
            log_path = os.path.join(config.OUTPUT_DIR, log_file)
            file_handler = logging.FileHandler(log_path, encoding='utf-8')
            file_handler.setLevel(log_level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        return logger
    
    @staticmethod
    def get_date_logger(name):
        """获取带日期的日志记录器
        
        Args:
            name: 日志记录器名称
            
        Returns:
            logging.Logger: 配置好的日志记录器
        """
        today = datetime.now().strftime('%Y-%m-%d')
        log_file = f"{name}_{today}.log"
        return LoggerManager.get_logger(name, log_file)

# 创建常用日志记录器
data_collection_logger = LoggerManager.get_logger('data_collection')
data_processing_logger = LoggerManager.get_logger('data_processing')
reporting_logger = LoggerManager.get_logger('reporting')
error_logger = LoggerManager.get_date_logger('error')