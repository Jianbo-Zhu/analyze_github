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
        self.DEFAULT_MAX_COUNT = 2000  # 默认最大记录数
        self.CHECK_INTERVAL = 50  # API检查间隔
        self.BUFFER_REMAINING = 10  # API剩余请求缓冲
        # 添加缓存字典来存储已获取的仓库对象和用户详情
        self.repo_cache = {}
        self.user_cache = {}
        # 添加缓存TTL，单位秒
        self.cache_ttl = 3600  # 缓存1小时
    
    def authenticate(self):
        """认证GitHub API"""
        try:
            if config.GITHUB_TOKEN:
                self.github = Github(config.GITHUB_TOKEN, per_page=100)
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
        # 确保github对象已初始化
        if not self.github:
            logger.warning("GitHub对象未初始化，尝试进行认证")
            auth_success = self.authenticate()
            if not auth_success:
                logger.error("认证失败，无法检查API速率限制")
                return
        
        try:
            remaining, limit = self.github.rate_limiting
            reset_time = self.github.rate_limiting_resettime
            
            logger.debug(f"API速率限制: 剩余 {remaining}/{limit} 次请求，重置时间: {datetime.fromtimestamp(reset_time)}")
            
            # 如果剩余请求次数不足，等待重置
            if remaining < self.BUFFER_REMAINING:
                wait_time = max(reset_time - time.time() + 5, self.rate_limit_wait)
                logger.warning(f"API请求次数即将耗尽，等待 {wait_time:.2f} 秒...")
                time.sleep(wait_time)
                logger.info("等待完成，继续执行")
        except Exception as e:
            logger.error(f"检查API速率限制时出错: {e}")
            # 发生错误时等待一段安全时间
            time.sleep(self.rate_limit_wait)
    
    def search_projects(self, query, sort='stars', order='desc'):
        """搜索GitHub项目
        
        Args:
            query: 搜索查询字符串
            sort: 排序字段 (stars, forks, updated)
            order: 排序顺序 (desc, asc)
            
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
                    # 缓存仓库对象
                    self._cache_repo(repo)
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
    
    def get_contributor_details(self, username):
        """获取贡献者详细信息
        
        Args:
            username: 用户名
            
        Returns:
            dict: 贡献者详细信息
        """
        # 检查缓存
        if username in self.user_cache:
            cached_data, timestamp = self.user_cache[username]
            # 检查缓存是否过期
            if time.time() - timestamp < self.cache_ttl:
                logger.debug(f"从缓存中获取用户 {username} 的详细信息")
                return cached_data
        
        try:
            self.check_rate_limit()
            user = self.github.get_user(username)
            user_data = {
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
            
            # 更新缓存
            self.user_cache[username] = (user_data, time.time())
            return user_data
        except Exception as e:
            logger.error(f"获取贡献者 {username} 详细信息时出错: {e}")
            # 出错时也缓存None，避免短时间内重复请求同一失败的用户
            self.user_cache[username] = (None, time.time())
            return None
    
    def get_repo(self, repo_id):
        """根据GitHub仓库ID获取仓库对象
        
        Args:
            repo_id: GitHub仓库ID
            
        Returns:
            Repository: GitHub仓库对象
        """
        # 检查缓存
        if repo_id in self.repo_cache:
            cached_repo, timestamp = self.repo_cache[repo_id]
            # 检查缓存是否过期
            if time.time() - timestamp < self.cache_ttl:
                logger.debug(f"从缓存中获取仓库ID {repo_id} 的对象")
                return cached_repo
        
        try:
            self.check_rate_limit()
            repo = self.github.get_repo(repo_id)
            logger.info(f"成功获取仓库ID {repo_id} 的对象: {repo.full_name}")
            
            # 更新缓存
            self._cache_repo(repo)
            return repo
        except Exception as e:
            logger.error(f"获取仓库ID {repo_id} 的对象时出错: {e}")
            raise
    
    def get_repo_by_name(self, full_name):
        """根据仓库全名获取仓库对象
        
        Args:
            full_name: 仓库全名，格式为'owner/repo'
            
        Returns:
            Repository: GitHub仓库对象
        """
        # 检查缓存 - 首先查找是否有相同full_name的缓存
        for repo_id, (repo, timestamp) in self.repo_cache.items():
            if hasattr(repo, 'full_name') and repo.full_name == full_name:
                if time.time() - timestamp < self.cache_ttl:
                    logger.debug(f"从缓存中获取仓库 {full_name} 的对象")
                    return repo
                break
        
        try:
            self.check_rate_limit()
            repo = self.github.get_repo(full_name)
            logger.info(f"成功获取仓库 {full_name} 的对象")
            
            # 更新缓存
            self._cache_repo(repo)
            return repo
        except Exception as e:
            logger.error(f"获取仓库 {full_name} 的对象时出错: {e}")
            raise
    
    def get_commits(self, repo, max_count=None, since_date=None):
        """获取项目的提交记录
        
        Args:
            repo: GitHub仓库对象
            max_count: 最大返回提交数量，None表示使用默认值
            since_date: 起始日期，只返回此日期之后的提交，None表示不限制
            
        Yields:
            Commit: GitHub提交对象
        """
        try:
            # 使用默认最大数量
            if max_count is None:
                max_count = self.DEFAULT_MAX_COUNT
            
            self.check_rate_limit()
            
            # 构建提交查询参数
            query_params = {}
            if since_date:
                query_params['since'] = since_date
            
            # 获取提交生成器
            commits_generator = repo.get_commits(**query_params)
            
            count = 0
            for commit in commits_generator:
                yield commit
                count += 1
                
                # 每处理CHECK_INTERVAL个提交检查一次速率限制
                if count % self.CHECK_INTERVAL == 0:
                    self.check_rate_limit()
                
                # 如果达到最大数量限制，停止迭代
                if count >= max_count:
                    logger.info(f"已达到最大提交数量限制 {max_count}，停止获取项目 {repo.full_name} 的提交记录")
                    break
                    
        except Exception as e:
            logger.error(f"获取项目 {repo.full_name} 提交记录时出错: {e}")
            return
    
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
    
    def get_pulls(self, repo, max_count=None, since_date=None):
        """获取项目的PR记录
        
        Args:
            repo: GitHub仓库对象
            max_count: 最大返回PR数量，None表示使用默认值
            since_date: 起始日期，只返回此日期之后的PR，None表示不限制
            
        Yields:
            PullRequest: GitHub PR对象
        """
        try:
            # 使用默认最大数量
            if max_count is None:
                max_count = self.DEFAULT_MAX_COUNT
            
            self.check_rate_limit()
            
            # 构建PR查询参数（不使用since参数，因为Repository.get_pulls()不支持）
            query_params = {
                'state': 'all',
                'sort': 'updated',
                'direction': 'desc'
            }
            
            # 获取PR生成器
            pulls_generator = repo.get_pulls(**query_params)
            
            count = 0
            for pr in pulls_generator:
                # 如果指定了起始日期，再次过滤确保PR创建时间在起始日期之后
                if since_date and pr.created_at < since_date:
                    break  # 由于按更新时间降序排序，一旦找到早于起始日期的PR，后续PR也会更早，可以停止迭代
                
                yield pr
                count += 1
                
                # 每处理CHECK_INTERVAL个PR检查一次速率限制
                if count % self.CHECK_INTERVAL == 0:
                    self.check_rate_limit()
                
                # 如果达到最大数量限制，停止迭代
                if count >= max_count:
                    logger.info(f"已达到最大PR数量限制 {max_count}，停止获取项目 {repo.full_name} 的PR记录")
                    break
                    
        except Exception as e:
            logger.error(f"获取项目 {repo.full_name} PR记录时出错: {e}")
            return
    
    def _cache_repo(self, repo):
        """缓存仓库对象
        
        Args:
            repo: GitHub仓库对象
        """
        if hasattr(repo, 'id'):
            self.repo_cache[repo.id] = (repo, time.time())


# 创建全局GitHub API实例
github_api = GitHubAPI()