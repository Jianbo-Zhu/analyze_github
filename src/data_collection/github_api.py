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
    
    def get_project_contributors(self, repo, max_count=None, since_date=None):
        """获取项目贡献者（使用生成器优化）
        
        Args:
            repo: GitHub仓库对象
            max_count: 最大返回贡献者数量，None表示返回全部
            since_date: 起始日期，只返回此日期之后有贡献的贡献者，None表示不限制
            
        Yields:
            Contributor: GitHub贡献者对象
        """
        try:
            self.check_rate_limit()
            
            # 获取贡献者生成器时添加错误处理
            try:
                # 如果指定了起始日期，我们需要通过其他方式筛选
                contributors_generator = repo.get_contributors()
            except GithubException as e:
                # 专门处理大型仓库的贡献者列表限制错误
                if hasattr(e, 'status') and e.status == 403:
                    error_message = str(e)
                    if "contributor list is too large" in error_message:
                        logger.warning(f"GitHub API限制：项目 {repo.full_name} 的贡献者列表过大，尝试使用分页模式获取部分贡献者")
                        
                        # 降级策略1：尝试只获取前50个贡献者（使用per_page参数）
                        try:
                            contributors_generator = repo.get_contributors(per_page=50)
                            logger.info(f"成功应用降级策略，尝试获取项目 {repo.full_name} 的部分贡献者")
                        except Exception as inner_e:
                            logger.warning(f"降级策略失败，无法获取项目 {repo.full_name} 的贡献者: {inner_e}")
                            # 放弃获取贡献者，直接返回空生成器
                            return
                    else:
                        logger.error(f"GitHub API错误（403）：获取项目 {repo.full_name} 贡献者时出错: {e}")
                        return
                else:
                    logger.error(f"GitHub API错误：获取项目 {repo.full_name} 贡献者时出错: {e}")
                    return
            except Exception as e:
                logger.error(f"获取项目 {repo.full_name} 贡献者生成器时出错: {e}")
                return
            
            count = 0
            filtered_count = 0
            try:
                for contributor in contributors_generator:
                    # 如果指定了起始日期，我们需要检查贡献者是否在该日期后有提交
                    if since_date:
                        # 获取该贡献者在指定日期后的提交
                        has_recent_contributions = False
                        try:
                            # 检查该贡献者是否在2025年以后有提交
                            recent_commits = list(repo.get_commits(author=contributor, since=since_date))
                            has_recent_contributions = len(recent_commits) > 0
                        except Exception as e:
                            logger.warning(f"检查贡献者 {contributor.login} 的近期提交时出错: {e}")
                            # 如果出错，保守起见跳过该贡献者
                            continue
                        
                        # 如果没有近期贡献，跳过
                        if not has_recent_contributions:
                            continue
                        filtered_count += 1
                    
                    yield contributor
                    count += 1
                    
                    # 每处理50个贡献者检查一次速率限制
                    if count % 50 == 0:
                        self.check_rate_limit()
                    
                    # 如果达到最大数量限制，停止迭代
                    if max_count is not None and count >= max_count:
                        logger.info(f"已达到最大贡献者数量限制 {max_count}，停止获取项目 {repo.full_name} 的贡献者")
                        break
                        
            except GithubException as e:
                # 处理迭代过程中的API错误
                if hasattr(e, 'status') and e.status == 403:
                    logger.warning(f"GitHub API限制：迭代项目 {repo.full_name} 贡献者时遇到限制，已处理 {count} 个贡献者")
                else:
                    logger.error(f"迭代项目 {repo.full_name} 贡献者时出错: {e}")
                    
            logger.debug(f"获取项目 {repo.full_name} 贡献者完成，共处理 {count} 个贡献者，过滤掉 {filtered_count} 个无近期贡献的贡献者")
                    
        except Exception as e:
            logger.error(f"获取项目 {repo.full_name} 贡献者信息时发生未预期错误: {e}")
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
            
            # 构建PR查询参数
            query_params = {
                'state': 'all',
                'sort': 'updated',
                'direction': 'desc'
            }
            if since_date:
                query_params['since'] = since_date
            
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

# 创建全局GitHub API实例
github_api = GitHubAPI()