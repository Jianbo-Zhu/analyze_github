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
    
    def execute_query(self, query, params=None, cursor_type=pymysql.cursors.DictCursor, fetch_all=True):
        """执行SQL查询
        
        Args:
            query: SQL查询语句
            params: 查询参数
            cursor_type: 游标类型
            fetch_all: 是否一次性获取所有结果
                      - True: 返回所有结果列表（默认行为，兼容性好）
                      - False: 返回生成器，流式获取结果，节省内存
                      - 'generator': 同False，返回生成器
                      - 'yield': 同False，返回生成器
        
        Returns:
            - 对于SELECT语句：当fetch_all=True时返回结果列表，否则返回生成器
            - 对于其他语句：返回受影响的行数
        """
        with self.get_cursor(cursor_type) as cursor:
            cursor.execute(query, params)
            
            # 处理SELECT查询
            if query.strip().upper().startswith('SELECT'):
                # 兼容旧代码，默认一次性获取所有结果
                if fetch_all is True:
                    return cursor.fetchall()
                else:
                    # 流式获取结果，返回生成器
                    def result_generator():
                        while True:
                            row = cursor.fetchone()
                            if row is None:
                                break
                            yield row
                    return result_generator()
            # 处理非SELECT语句
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