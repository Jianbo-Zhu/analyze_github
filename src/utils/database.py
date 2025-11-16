import pymysql
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class DatabaseManager:
    """数据库管理类"""
    
    def __init__(self):
        # 延迟导入以避免循环依赖
        from .config import config
        self.connection_params = config.DB_CONNECTION_STRING
        self.connection = None
    
    def connect(self):
        """建立数据库连接"""
        try:
            self.connection = pymysql.connect(**self.connection_params)
            logger.info(f"成功连接到数据库: {self.connection_params['host']}:{self.connection_params['port']}/{self.connection_params['database']}")
            return self.connection
        except pymysql.MySQLError as e:
            logger.error(f"数据库连接失败: {e}")
            raise
    
    def disconnect(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
            logger.info("数据库连接已关闭")
    
    @contextmanager
    def get_cursor(self, cursor_type=pymysql.cursors.DictCursor):
        """获取数据库游标上下文管理器"""
        if not self.connection:
            self.connect()
        
        try:
            with self.connection.cursor(cursor_type) as cursor:
                yield cursor
                self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            logger.error(f"数据库操作失败: {e}")
            raise
    
    def execute_query(self, query, params=None, cursor_type=pymysql.cursors.DictCursor):
        """执行SQL查询"""
        with self.get_cursor(cursor_type) as cursor:
            cursor.execute(query, params)
            if query.strip().upper().startswith('SELECT'):
                return cursor.fetchall()
            else:
                return cursor.rowcount
    
    def execute_many(self, query, params_list):
        """批量执行SQL查询"""
        with self.get_cursor() as cursor:
            cursor.executemany(query, params_list)
            return cursor.rowcount
    
    def is_table_empty(self, table_name):
        """检查表是否为空"""
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = self.execute_query(query)
        return result[0]['count'] == 0
    
    def get_last_insert_id(self):
        """获取最后插入的ID"""
        with self.get_cursor() as cursor:
            cursor.execute("SELECT LAST_INSERT_ID()")
            return cursor.fetchone()['LAST_INSERT_ID()']

# 创建全局数据库管理器实例
db_manager = DatabaseManager()