from github import Github, GithubException
import time
import logging
from datetime import datetime
from src.utils.config import config
from src.utils.logger import data_collection_logger

logger = data_collection_logger

class GitHubAPI:
    """GitHub API交互类"""
    
    def __init__(self):
        self.github = None
        self.rate_limit_wait = config.GITHUB_API_RATE_LIMIT_WAIT
    
    def authenticate(self):
        """认证GitHub API"""
        try:
            if config.GITHUB_TOKEN:
                self.github = Github(config.GITHUB_TOKEN)
                logger.info("使用GitHub Token进行认证")
            else:
                self.github = Github()
                logger.warning("未使用Token认证，将受到更严格的API速率限制")
            
            # 检查认证状态
            user = self.github.get_user()
            logger.info(f"认证成功，当前API剩余请求次数: {self.github.rate_limiting[0]}/{self.github.rate_limiting[1]}")
            return True
        except Exception as e:
            logger.error(f"GitHub认证失败: {e}")
            return False
    
    def check_rate_limit(self):
        """检查并处理API速率限制"""
        remaining, limit = self.github.rate_limiting
        reset_time = self.github.rate_limiting_resettime
        
        logger.info(f"API速率限制: 剩余 {remaining}/{limit} 次请求，重置时间: {datetime.fromtimestamp(reset_time)}")
        
        # 如果剩余请求次数不足，等待重置
        if remaining < 10:
            wait_time = max(reset_time - time.time() + 10, self.rate_limit_wait)
            logger.warning(f"API请求次数即将耗尽，等待 {wait_time:.2f} 秒...")
            time.sleep(wait_time)
            logger.info("等待完成，继续执行")
    
    def search_projects(self, query, sort='stars', order='desc', per_page=100):
        """搜索GitHub项目
        
        Args:
            query: 搜索查询字符串
            sort: 排序字段 (stars, forks, updated)
            order: 排序顺序 (desc, asc)
            per_page: 每页结果数量
            
        Yields:
            Repository: GitHub仓库对象
        """
        if not self.github:
            self.authenticate()
        
        logger.info(f"开始搜索项目: query={query}, sort={sort}, order={order}")
        
        try:
            # 执行搜索
            search_results = self.github.search_repositories(
                query=query,
                sort=sort,
                order=order
            )
            
            count = 0
            for repo in search_results:
                # 检查速率限制
                self.check_rate_limit()
                logger.info(f"开始处理： {repo.full_name}")
                
                # 过滤项目（根据PRD要求）
                if self._is_valid_project(repo):
                    count += 1
                    logger.info(f"找到符合条件的项目 #{count}: {repo.full_name}")
                    yield repo
                
                # 达到最大项目数时停止
                if count >= config.MAX_PROJECTS:
                    logger.info(f"已达到最大项目数 {config.MAX_PROJECTS}，停止搜索")
                    break
                
        except GithubException as e:
            logger.error(f"搜索项目时发生错误: {e}")
            raise
    
    def _is_valid_project(self, repo):
        """验证项目是否符合条件
        
        Args:
            repo: GitHub仓库对象
            
        Returns:
            bool: 是否符合条件
        """
        # 检查星标数和Fork数
        if repo.stargazers_count < config.MIN_STARS or repo.forks_count < config.MIN_FORKS:
            return False
        
        # 不再检查提交次数，直接返回True
        return True
    
    def get_project_languages(self, repo):
        """获取项目使用的编程语言
        
        Args:
            repo: GitHub仓库对象
            
        Returns:
            dict: 语言名称到字节数的映射
        """
        try:
            self.check_rate_limit()
            languages = repo.get_languages()
            return languages
        except Exception as e:
            logger.error(f"获取项目 {repo.full_name} 语言信息时出错: {e}")
            return {}
    
    def get_project_contributors(self, repo, max_count=None):
        """获取项目贡献者（使用生成器优化）
        
        Args:
            repo: GitHub仓库对象
            max_count: 最大返回贡献者数量，None表示返回全部
            
        Yields:
            Contributor: GitHub贡献者对象
        """
        try:
            self.check_rate_limit()
            
            # 获取贡献者生成器
            contributors_generator = repo.get_contributors()
            
            count = 0
            for contributor in contributors_generator:
                yield contributor
                count += 1
                
                # 每处理50个贡献者检查一次速率限制
                if count % 50 == 0:
                    self.check_rate_limit()
                
                # 如果达到最大数量限制，停止迭代
                if max_count is not None and count >= max_count:
                    break
                    
        except Exception as e:
            logger.error(f"获取项目 {repo.full_name} 贡献者信息时出错: {e}")
            return
    
    def get_contributor_details(self, username):
        """获取贡献者详细信息
        
        Args:
            username: 用户名
            
        Returns:
            dict: 贡献者详细信息
        """
        try:
            self.check_rate_limit()
            user = self.github.get_user(username)
            return {
                'id': user.id,
                'username': user.login,
                'name': user.name,
                'email': user.email,
                'location': user.location,
                'company': user.company,
                'avatar_url': user.avatar_url,
                'html_url': user.html_url,
                'created_at': user.created_at
            }
        except Exception as e:
            logger.error(f"获取贡献者 {username} 详细信息时出错: {e}")
            return None
    
    # 注意：get_recent_commits方法已被移除，不再获取提交数据
    
    def get_project_topics(self, repo):
        """获取项目主题标签
        
        Args:
            repo: GitHub仓库对象
            
        Returns:
            list: 主题标签列表
        """
        try:
            self.check_rate_limit()
            topics = repo.get_topics()
            return topics
        except Exception as e:
            logger.error(f"获取项目 {repo.full_name} 主题标签时出错: {e}")
            return []

# 创建全局GitHub API实例
github_api = GitHubAPI()