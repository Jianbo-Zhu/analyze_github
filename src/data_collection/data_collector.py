import time
import logging
from datetime import datetime
from src.data_collection.github_api import github_api
from src.utils.database import db_manager
from src.utils.logger import data_collection_logger
from src.utils.config import config

logger = data_collection_logger

class DataCollector:
    """GitHub数据采集器"""
    
    def __init__(self):
        self.github_api = github_api
        self.db_manager = db_manager
    
    def initialize_collection(self):
        """初始化数据采集过程"""
        logger.info("开始初始化数据采集...")
        
        # 认证GitHub API
        if not self.github_api.authenticate():
            raise Exception("GitHub API认证失败，无法继续采集数据")
        
        # 检查数据库连接
        try:
            self.db_manager.connect()
            logger.info("数据库连接成功")
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            raise
        
        logger.info("数据采集初始化完成")
    
    def collect_projects(self):
        """采集符合条件的GitHub项目"""
        logger.info(f"开始采集GitHub项目，最大项目数: {config.MAX_PROJECTS}")
        logger.info(f"筛选条件: 星标数>={config.MIN_STARS}, Fork数>={config.MIN_FORKS}")
        
        # 构建搜索查询
        query = f"pushed:>{config.START_DATE}"
        
        try:
            # 搜索项目
            for repo in self.github_api.search_projects(query):
                # 保存项目信息
                self._save_project(repo)
                
                # 保存项目语言信息
                self._save_project_languages(repo)
                
                # 保存项目主题标签
                self._save_project_topics(repo)
                
                # 保存项目统计信息
                self._save_project_statistics(repo)
                
                # 保存贡献者信息，但允许失败继续
                try:
                    self._save_contributors(repo)
                except Exception as e:
                    logger.warning(f"保存项目 {repo.full_name} 贡献者信息时出错，继续处理其他数据: {e}")
                
                logger.info(f"项目 {repo.full_name} 数据采集完成")
                
        except Exception as e:
            logger.error(f"采集项目时发生错误: {e}")
            raise
        finally:
            self.db_manager.disconnect()
    
    def _save_project(self, repo):
        """保存项目基本信息"""
        try:
            # 检查项目是否已存在
            query = "SELECT id FROM projects WHERE github_id = %s"
            result = self.db_manager.execute_query(query, (repo.id,))
            
            if result:
                project_id = result[0]['id']
                logger.debug(f"项目 {repo.full_name} 已存在，ID: {project_id}")
                return project_id
            
            # 插入新项目
            query = """
            INSERT INTO projects (
                github_id, name, full_name, description, created_at, 
                updated_at, pushed_at, stargazers_count, forks_count, 
                open_issues_count, license_name, homepage, default_branch,
                contributors_count, main_language, topics, created_at_timestamp,
                updated_at_timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # 初始设置贡献者数量为0，将在_save_contributors方法中更新为2025年以来的贡献者数量
            contributors_count = 0
            
            params = (
                repo.id,
                repo.name,
                repo.full_name,
                repo.description,
                repo.created_at,
                repo.updated_at,
                repo.pushed_at,
                repo.stargazers_count,
                repo.forks_count,
                repo.open_issues_count,
                repo.license.name if repo.license else None,
                repo.homepage,
                repo.default_branch,
                contributors_count,
                repo.language,
                ','.join(self.github_api.get_project_topics(repo)),
                int(repo.created_at.timestamp()) if repo.created_at else None,
                int(repo.updated_at.timestamp()) if repo.updated_at else None
            )
            
            self.db_manager.execute_query(query, params)
            project_id = self.db_manager.get_last_insert_id()
            logger.debug(f"保存项目 {repo.full_name} 成功，ID: {project_id}")
            
            return project_id
            
        except Exception as e:
            logger.error(f"保存项目 {repo.full_name} 时出错: {e}")
            raise
    
    def _save_project_languages(self, repo):
        """保存项目语言信息"""
        try:
            # 获取项目ID
            query = "SELECT id FROM projects WHERE github_id = %s"
            result = self.db_manager.execute_query(query, (repo.id,))
            if not result:
                return
            
            project_id = result[0]['id']
            
            # 获取语言信息
            languages = self.github_api.get_project_languages(repo)
            if not languages:
                return
            
            # 计算总字节数
            total_bytes = sum(languages.values())
            
            # 插入语言数据
            for language_name, bytes_count in languages.items():
                percentage = (bytes_count / total_bytes) * 100 if total_bytes > 0 else 0
                
                query = """
                INSERT INTO languages (project_id, language_name, bytes_count, percentage)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE bytes_count = %s, percentage = %s
                """
                
                params = (project_id, language_name, bytes_count, percentage, bytes_count, percentage)
                self.db_manager.execute_query(query, params)
            
            logger.debug(f"保存项目 {repo.full_name} 语言信息成功")
            
        except Exception as e:
            logger.error(f"保存项目 {repo.full_name} 语言信息时出错: {e}")
    
    def _save_project_topics(self, repo):
        """保存项目主题标签"""
        try:
            # 获取项目ID
            query = "SELECT id FROM projects WHERE github_id = %s"
            result = self.db_manager.execute_query(query, (repo.id,))
            if not result:
                return
            
            project_id = result[0]['id']
            
            # 获取主题标签
            topics = self.github_api.get_project_topics(repo)
            
            # 插入主题标签数据
            for topic in topics:
                query = """
                INSERT IGNORE INTO topics (project_id, topic_name)
                VALUES (%s, %s)
                """
                
                self.db_manager.execute_query(query, (project_id, topic))
            
            logger.debug(f"保存项目 {repo.full_name} 主题标签成功")
            
        except Exception as e:
            logger.error(f"保存项目 {repo.full_name} 主题标签时出错: {e}")
    
    def _count_pulls(self, repo):
        """计算PR数量（使用迭代器而非一次性加载所有PR）"""
        try:
            pulls_generator = repo.get_pulls(state='all')
            count = 0
            # 只计数不保存整个列表
            for _ in pulls_generator:
                count += 1
                # 每处理100个PR检查一次速率限制
                if count % 100 == 0:
                    self.github_api.check_rate_limit()
            return count
        except Exception as e:
            logger.error(f"计算项目 {repo.full_name} PR数量时出错: {e}")
            return 0
    
    def _save_project_statistics(self, repo):
        """保存项目统计信息"""
        try:
            # 获取项目ID
            query = "SELECT id FROM projects WHERE github_id = %s"
            result = self.db_manager.execute_query(query, (repo.id,))
            if not result:
                return
            
            project_id = result[0]['id']
            
            # 获取统计信息
            stats = {
                'total_commits': None,  # 不再获取提交数量
                'total_issues': repo.open_issues_count,
                'total_pulls': self._count_pulls(repo),  # 使用优化后的方法计算PR数量
                'contributors_count': 0,  # 避免再次触发API调用，使用默认值
                'watchers_count': repo.subscribers_count,
                'network_count': repo.network_count,
                'created_month': repo.created_at.strftime('%Y-%m') if repo.created_at else None,
                'updated_month': repo.updated_at.strftime('%Y-%m') if repo.updated_at else None
            }
            
            # 插入或更新统计数据（total_commits设置为NULL）
            query = """
            INSERT INTO statistics (
                project_id, total_commits, total_issues, total_pulls, 
                contributors_count, watchers_count, network_count, 
                created_month, updated_month
            ) VALUES (%s, NULL, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                total_issues = %s, total_pulls = %s,
                contributors_count = %s, watchers_count = %s, network_count = %s,
                created_month = %s, updated_month = %s
            """
            
            params = (
                project_id,
                # 不再使用total_commits，直接设置为NULL
                stats['total_issues'],
                stats['total_pulls'],
                stats['contributors_count'],
                stats['watchers_count'],
                stats['network_count'],
                stats['created_month'],
                stats['updated_month'],
                # 更新参数
                stats['total_issues'],
                stats['total_pulls'],
                stats['contributors_count'],
                stats['watchers_count'],
                stats['network_count'],
                stats['created_month'],
                stats['updated_month']
            )
            
            self.db_manager.execute_query(query, params)
            logger.debug(f"保存项目 {repo.full_name} 统计信息成功")
            
        except Exception as e:
            logger.error(f"保存项目 {repo.full_name} 统计信息时出错: {e}")
    
    def _save_contributors(self, repo):
        """保存项目贡献者信息"""
        try:
            # 获取项目ID
            query = "SELECT id FROM projects WHERE github_id = %s"
            result = self.db_manager.execute_query(query, (repo.id,))
            if not result:
                return
            
            project_id = result[0]['id']
            
            # 设置起始日期为2025年1月1日
            import datetime
            since_date = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
            logger.info(f"开始获取项目 {repo.full_name} 的2025年以来的贡献者")
            
            # 使用生成器获取贡献者，避免一次性加载所有贡献者
            # 限制处理最多1000个贡献者，避免处理过多数据
            contributors_processed = 0
            for contributor in self.github_api.get_project_contributors(repo, max_count=1000, since_date=since_date):
                # 保存贡献者信息
                contributor_id = self._save_contributor(contributor)
                
                if contributor_id:
                    # 保存项目-贡献者关联
                    self._save_project_contributor(project_id, contributor_id, contributor.contributions)
                    
                contributors_processed += 1
                
                # 每处理100个贡献者检查一次
                if contributors_processed % 100 == 0:
                    logger.debug(f"已处理项目 {repo.full_name} 的 {contributors_processed} 个贡献者")
            
            logger.info(f"保存项目 {repo.full_name} 贡献者信息成功，共处理 {contributors_processed} 个2025年以来的贡献者")
            
            # 更新项目的贡献者数量
            query = "UPDATE projects SET contributors_count = %s WHERE id = %s"
            self.db_manager.execute_query(query, (contributors_processed, project_id))
            
        except Exception as e:
            # 专门处理大型仓库的API限制错误
            error_message = str(e)
            if "contributor list is too large" in error_message or "403" in error_message:
                logger.warning(f"GitHub API限制：无法获取项目 {repo.full_name} 的贡献者列表，这是因为仓库历史或贡献者列表过大")
                # 在这里可以添加降级策略，比如只获取前N个贡献者
            else:
                logger.error(f"保存项目 {repo.full_name} 贡献者信息时出错: {e}")
            # 即使贡献者获取失败，也不中断整个项目的处理
    
    def _save_contributor(self, contributor):
        """保存贡献者基本信息"""
        try:
            # 检查贡献者是否已存在
            query = "SELECT id FROM contributors WHERE github_id = %s"
            result = self.db_manager.execute_query(query, (contributor.id,))
            
            if result:
                return result[0]['id']
            
            # 获取详细信息
            details = self.github_api.get_contributor_details(contributor.login)
            
            # 插入新贡献者
            query = """
            INSERT INTO contributors (
                github_id, username, avatar_url, html_url, 
                contributions, email, location, company, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            params = (
                contributor.id,
                contributor.login,
                contributor.avatar_url,
                contributor.html_url,
                contributor.contributions,
                details.get('email') if details else None,
                details.get('location') if details else None,
                details.get('company') if details else None,
                details.get('created_at') if details else None
            )
            
            self.db_manager.execute_query(query, params)
            return self.db_manager.get_last_insert_id()
            
        except Exception as e:
            logger.error(f"保存贡献者 {contributor.login} 时出错: {e}")
            return None
    
    def _save_project_contributor(self, project_id, contributor_id, contributions):
        """保存项目-贡献者关联"""
        try:
            query = """
            INSERT INTO project_contributors (project_id, contributor_id, contributions)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE contributions = %s
            """
            
            self.db_manager.execute_query(query, (project_id, contributor_id, contributions, contributions))
            
        except Exception as e:
            logger.error(f"保存项目-贡献者关联时出错: {e}")
    
    # 注意：_save_recent_commits方法已被移除，不再保存提交数据

# 创建数据采集器实例
data_collector = DataCollector()