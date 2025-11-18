import time
import logging
from datetime import datetime, timezone
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
        # 提取为类常量，避免重复定义
        # 注意：用户确认2025年是正确的时间设置
        self.since_date = datetime(2025, 1, 1, tzinfo=timezone.utc)
        self.DEFAULT_MAX_COUNT = 2000  # 默认最大记录数
        self.CHECK_INTERVAL = 100  # 检查间隔
        logger.info("数据采集器初始化完成")
    
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
        """采集符合条件的GitHub项目，支持断点续传功能"""
        logger.info(f"开始采集GitHub项目，最大项目数: {config.MAX_PROJECTS}")
        logger.info(f"筛选条件: 星标数>={config.MIN_STARS}, Fork数>={config.MIN_FORKS}")
        
        try:
            # 检查projects表中当前记录数量
            query = "SELECT COUNT(*) as count FROM projects"
            result = self.db_manager.execute_query(query)
            current_count = result[0]['count'] if result else 0
            logger.info(f"当前projects表中已有 {current_count} 条记录")
            
            # 如果当前记录数小于最大限制，则拉取新项目
            if current_count < config.MAX_PROJECTS:
                # 第一步：先拉取所有符合条件的仓库并存入projects表
                self._fetch_all_projects()
            else:
                logger.info(f"projects表中已有 {current_count} 条记录，已达到最大限制 {config.MAX_PROJECTS}，跳过新项目拉取")
            
            # 第二步：处理状态为pending或failed的项目
            self._process_pending_projects()
            
        except Exception as e:
            logger.error(f"采集项目时发生错误: {e}")
            raise
        finally:
            self.db_manager.disconnect()
    
    def _fetch_all_projects(self):
        """拉取所有符合条件的仓库并存入projects表，不进行详细数据采集"""
        logger.info("开始拉取所有符合条件的GitHub仓库")
        
        # 构建搜索查询
        query = f"pushed:>{config.START_DATE}"
        
        try:
            # 搜索项目并只保存基本信息
            projects_saved = 0
            for repo in self.github_api.search_projects(query):
                try:
                    self._save_project(repo)
                    projects_saved += 1
                    logger.info(f"已保存仓库基本信息: {repo.full_name} (共{projects_saved}个)")
                    
                    # 检查是否达到最大项目数
                    if projects_saved >= config.MAX_PROJECTS:
                        logger.info(f"已达到最大项目数限制: {config.MAX_PROJECTS}")
                        break
                except Exception as e:
                    logger.warning(f"保存仓库 {repo.full_name} 基本信息时出错，继续处理下一个仓库: {e}")
                    continue
            
            logger.info(f"仓库基本信息拉取完成，共保存 {projects_saved} 个仓库")
            
        except Exception as e:
            logger.error(f"拉取仓库基本信息时发生错误: {e}")
            raise
    
    def _process_pending_projects(self):
        """处理状态为pending或failed的项目，进行详细数据采集"""
        logger.info("开始处理待采集的项目")
        
        try:
            # 查询状态为pending或failed的项目
            query = """
            SELECT github_id, name, full_name 
            FROM projects 
            WHERE status IN ('pending', 'failed') 
            ORDER BY status ASC, id ASC
            """
            pending_projects = self.db_manager.execute_query(query)
            
            if not pending_projects:
                logger.info("没有待处理的项目")
                return
            
            logger.info(f"找到 {len(pending_projects)} 个待处理的项目")
            
            # 处理每个待处理的项目
            for project in pending_projects:
                project_id = project['github_id']
                project_name = project['full_name']
                logger.info(f"开始处理项目: {project_name}")
                
                try:
                    # 获取GitHub仓库对象
                    repo = self.github_api.get_repo(project_id)
                    
                    # 更新项目状态为采集ing
                    self._update_project_status(project_id, 'collecting')
                    
                    # 采集详细数据
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
                        logger.warning(f"保存项目 {project_name} 贡献者信息时出错，继续处理其他数据: {e}")
                    
                    # 更新项目状态为完成
                    self._update_project_status(project_id, 'completed')
                    logger.info(f"项目 {project_name} 数据采集完成")
                    
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"处理项目 {project_name} 时出错: {error_msg}")
                    # 更新项目状态为失败并记录错误信息
                    self._update_project_status(project_id, 'failed', error_msg)
                    # 继续处理下一个项目
                    continue
            
            logger.info("所有待处理项目已处理完成")
            
        except Exception as e:
            logger.error(f"处理待采集项目时发生错误: {e}")
            raise
    
    def _update_project_status(self, github_id, status, error_msg=None):
        """更新项目的采集状态"""
        try:
            if status == 'failed' and error_msg:
                query = "UPDATE projects SET status = %s, last_error = %s WHERE github_id = %s"
                self.db_manager.execute_query(query, (status, error_msg, github_id))
            else:
                query = "UPDATE projects SET status = %s, last_error = NULL WHERE github_id = %s"
                self.db_manager.execute_query(query, (status, github_id))
            logger.debug(f"已更新项目ID {github_id} 的状态为: {status}")
        except Exception as e:
            logger.error(f"更新项目 {github_id} 状态时出错: {e}")
    
    def get_repo(self, github_id):
        """根据GitHub ID获取仓库对象"""
        try:
            query = "SELECT full_name FROM projects WHERE github_id = %s"
            result = self.db_manager.execute_query(query, (github_id,))
            if result:
                full_name = result[0]['full_name']
                return self.github_api.get_repo_by_name(full_name)
            return None
        except Exception as e:
            logger.error(f"获取仓库对象时出错: {e}")
            raise
    
    def _save_project(self, repo):
        """保存项目基本信息"""
        try:
            logger.info(f"开始处理项目基本信息: {repo.full_name}")
            # 检查项目是否已存在
            query = "SELECT id, status FROM projects WHERE github_id = %s"
            result = self.db_manager.execute_query(query, (repo.id,))
            
            if result:
                project_id = result[0]['id']
                status = result[0]['status']
                logger.debug(f"项目 {repo.full_name} 已存在，ID: {project_id}，状态: {status}")
                # 如果项目状态是failed，重置为pending以便重新处理
                if status == 'failed':
                    query = "UPDATE projects SET status = 'pending', last_error = NULL WHERE id = %s"
                    self.db_manager.execute_query(query, (project_id,))
                    logger.info(f"已重置项目 {repo.full_name} 的状态为pending")
                return project_id
            
            # 插入新项目
            query = """
            INSERT INTO projects (
                github_id, name, full_name, description, created_at, 
                updated_at, pushed_at, stargazers_count, forks_count, 
                open_issues_count, license_name, homepage, default_branch,
                contributors_count, main_language, topics, created_at_timestamp,
                updated_at_timestamp, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                int(repo.updated_at.timestamp()) if repo.updated_at else None,
                'pending'  # 初始状态为待采集
            )
            
            self.db_manager.execute_query(query, params)
            project_id = self.db_manager.get_last_insert_id()
            logger.info(f"项目基本信息处理完成: {repo.full_name}，ID: {project_id}")
            
            return project_id
            
        except Exception as e:
            logger.error(f"保存项目 {repo.full_name} 时出错: {e}")
            raise
    
    def _save_project_languages(self, repo):
        """保存项目语言信息"""
        try:
            logger.info(f"开始处理项目语言信息: {repo.full_name}")
            # 获取项目ID
            query = "SELECT id FROM projects WHERE github_id = %s"
            result = self.db_manager.execute_query(query, (repo.id,))
            if not result:
                logger.info(f"项目 {repo.full_name} 不存在，跳过语言信息处理")
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
            
            logger.info(f"项目语言信息处理完成: {repo.full_name}")
            
        except Exception as e:
            logger.error(f"保存项目 {repo.full_name} 语言信息时出错: {e}")
    
    def _save_project_topics(self, repo):
        """保存项目主题标签"""
        try:
            logger.info(f"开始处理项目主题标签: {repo.full_name}")
            # 获取项目ID
            query = "SELECT id FROM projects WHERE github_id = %s"
            result = self.db_manager.execute_query(query, (repo.id,))
            if not result:
                logger.info(f"项目 {repo.full_name} 不存在，跳过主题标签处理")
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
            
            logger.info(f"项目主题标签处理完成: {repo.full_name}")
            
        except Exception as e:
            logger.error(f"保存项目 {repo.full_name} 主题标签时出错: {e}")
    

    
    def _save_project_statistics(self, repo):
        """保存项目统计信息"""
        try:
            logger.info(f"开始处理项目统计信息: {repo.full_name}")
            # 获取项目ID
            query = "SELECT id FROM projects WHERE github_id = %s"
            result = self.db_manager.execute_query(query, (repo.id,))
            if not result:
                logger.info(f"项目 {repo.full_name} 不存在，跳计统计信息处理")
                return
            
            project_id = result[0]['id']
            
            # 获取统计信息
            stats = {
                'total_commits': None,  # 不再获取提交数量
                'total_issues': repo.open_issues_count,
                'total_pulls': 0,  # 先设置为0，后续在保存PR数据时会更新
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
                total_issues = %s, 
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
                stats['contributors_count'],
                stats['watchers_count'],
                stats['network_count'],
                stats['created_month'],
                stats['updated_month']
            )
            
            self.db_manager.execute_query(query, params)
            logger.info(f"项目统计信息处理完成: {repo.full_name}")
            
        except Exception as e:
            logger.error(f"保存项目 {repo.full_name} 统计信息时出错: {e}")
    
    def _save_pull_request(self, pr, project_id):
        """保存单个PR记录信息
        
        Args:
            pr: GitHub PR对象
            project_id: 项目ID
        """
        try:
            logger.info(f"开始处理PR记录: #{pr.number} - {pr.title[:50]}..." if len(pr.title) > 50 else f"开始处理PR记录: #{pr.number} - {pr.title}")
            
            # 检查PR是否已存在
            query = "SELECT id FROM pull_requests WHERE project_id = %s AND pr_number = %s"
            result = self.db_manager.execute_query(query, (project_id, pr.number))
            
            if result:
                logger.info(f"PR记录 #{pr.number} 已存在，跳过处理")
                return
            
            # 获取PR创建者的ID
            creator_id = None
            if pr.user:
                # 创建临时贡献者对象
                class TempContributor:
                    def __init__(self, user):
                        self.id = user.id
                        self.login = user.login
                        self.avatar_url = user.avatar_url
                        self.html_url = user.html_url
                        self.contributions = 0  # PR创建不计入贡献数
                
                temp_contributor = TempContributor(pr.user)
                creator_id = self._save_contributor(temp_contributor)
            
            # 获取PR详情（包含commits_count、additions、deletions、changed_files）
            # 避免重复API调用，直接使用已有信息
            merged_at = pr.merged_at if hasattr(pr, 'merged_at') and pr.merged_at else None
            merged = pr.merged if hasattr(pr, 'merged') else False
            
            # 准备PR数据
            # 添加基础统计信息，详细信息可以通过单独调用获取
            commits_count = 0
            additions = 0
            deletions = 0
            changed_files = 0
            
            # 尝试获取更详细信息但不强制依赖
            try:
                pr_details = pr.as_pull_request()  # 转换为完整的PullRequest对象
                commits_count = pr_details.commits if pr_details else 0
                additions = pr_details.additions if pr_details else 0
                deletions = pr_details.deletions if pr_details else 0
                changed_files = pr_details.changed_files if pr_details else 0
            except Exception as e:
                logger.warning(f"获取PR #{pr.number} 详情时出错: {e}，使用默认值")
            
            # 插入新PR记录
            query = """
            INSERT INTO pull_requests (
                project_id, pr_number, title, body, state, creator_id, 
                created_at, updated_at, closed_at, merged_at, merged,
                commits_count, additions, deletions, changed_files
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            params = (
                project_id,
                pr.number,
                pr.title,
                pr.body,
                pr.state,
                creator_id,
                pr.created_at,
                pr.updated_at,
                pr.closed_at,
                pr.merged_at,
                pr.merged,
                commits_count,
                additions,
                deletions,
                changed_files
            )
            
            self.db_manager.execute_query(query, params)
            logger.info(f"PR记录处理完成: #{pr.number}")
            
        except Exception as e:
            logger.error(f"保存PR记录 #{pr.number} 时出错: {e}")
    
    def _save_pull_requests(self, repo):
        """保存项目的PR记录（仅保存2025年以来的）"""
        try:
            logger.info(f"开始保存项目 {repo.full_name} 的PR记录（仅2025年来）")
            
            # 获取项目ID
            query = "SELECT id FROM projects WHERE github_id = %s"
            result = self.db_manager.execute_query(query, (repo.id,))
            if not result:
                logger.info(f"项目 {repo.full_name} 不存在，跳过PR记录处理")
                return 0
            
            project_id = result[0]['id']
            
            # 从PR数据中获取并保存PR记录
            prs_processed = 0
            for pr in self.github_api.get_pulls(repo, max_count=self.DEFAULT_MAX_COUNT, since_date=self.since_date):
                try:
                    self._save_pull_request(pr, project_id)
                    prs_processed += 1
                    
                    # 每处理CHECK_INTERVAL个PR检查一次
                    if prs_processed % self.CHECK_INTERVAL == 0:
                        logger.debug(f"已处理项目 {repo.full_name} 的 {prs_processed} 个PR记录")
                        # 定期更新统计信息，避免中途出错导致数据不一致
                        query = "UPDATE statistics SET total_pulls = %s WHERE project_id = %s"
                        self.db_manager.execute_query(query, (prs_processed, project_id))
                        logger.debug(f"已更新项目ID {project_id} 的PR统计数量: {prs_processed}")
                except Exception as e:
                    logger.error(f"处理PR #{pr.number} 时出错: {e}，继续处理下一个PR")
                    continue
            
            # 更新项目统计信息中的PR数量
            if prs_processed > 0:
                query = "UPDATE statistics SET total_pulls = %s WHERE project_id = %s"
                self.db_manager.execute_query(query, (prs_processed, project_id))
                logger.info(f"已更新项目 {repo.full_name} 的PR统计数量为 {prs_processed}")
            
            logger.info(f"项目 {repo.full_name} 的PR记录保存完成，共处理 {prs_processed} 个PR")
            return prs_processed
            
        except Exception as e:
            logger.error(f"保存项目 {repo.full_name} 的PR记录时出错: {e}")
            return 0
    
    def _save_contributors(self, repo):
        """保存项目贡献者信息，优先使用从commit数据获取的方式"""
        try:
            # 获取项目ID
            query = "SELECT id FROM projects WHERE github_id = %s"
            result = self.db_manager.execute_query(query, (repo.id,))
            if not result:
                return
            
            project_id = result[0]['id']
            
            # 尝试从commit数据获取贡献者信息
            contributors_processed = self._save_contributors_from_commits(repo, project_id)
            
            # 如果从commit获取失败，尝试使用传统方式作为备用
            if contributors_processed == 0:
                logger.info(f"从commit数据获取贡献者信息失败或无贡献者，尝试使用传统方式获取项目 {repo.full_name} 的贡献者")
                contributors_processed = self._save_contributors_from_api(repo, project_id)
            
            logger.info(f"保存项目 {repo.full_name} 贡献者信息成功，共处理 {contributors_processed} 个贡献者")
            
            # 更新项目的贡献者数量
            query = "UPDATE projects SET contributors_count = %s WHERE id = %s"
            self.db_manager.execute_query(query, (contributors_processed, project_id))
            
        except Exception as e:
            logger.error(f"保存项目 {repo.full_name} 贡献者信息时出错: {e}")
            # 即使贡献者获取失败，也不中断整个项目的处理
    
    def _save_contributors_from_commits(self, repo, project_id):
        """从commit数据中获取并保存贡献者信息和提交数据
        
        Args:
            repo: GitHub仓库对象
            project_id: 项目ID
            
        Returns:
            int: 处理的贡献者数量
        """
        try:
            # 使用类中已定义的since_date
            since_date = self.since_date
            logger.info(f"开始从commit数据获取项目 {repo.full_name} 的2025年以来的贡献者和提交记录")
            
            # 用于跟踪已处理的贡献者，避免重复处理
            processed_contributors = set()
            contributors_processed = 0
            commits_processed = 0
            
            # 从commit数据中获取贡献者信息和提交数据
            for commit in self.github_api.get_commits(repo, max_count=2000, since_date=since_date):
                try:
                    # 获取提交作者信息
                    commit_author = commit.commit.author
                    commit_sha = commit.sha
                    commit_message = commit.commit.message
                    commit_date = commit.commit.author.date
                    
                    # 尝试获取GitHub用户信息
                    contributor = None
                    contributor_id = None
                    if commit.author:
                        contributor = commit.author
                    
                    # 处理贡献者信息
                    if not contributor and commit_author:
                        # 使用作者名称和邮箱作为标识
                        temp_contributor_id = f"{commit_author.name}-{commit_author.email}" if commit_author.email else commit_author.name
                        
                        # 如果是新贡献者，保存其信息
                        if temp_contributor_id not in processed_contributors:
                            # 创建一个临时对象来保存必要的信息
                            class TempContributor:
                                def __init__(self, name, email):
                                    self.id = name + (email if email else '')
                                    self.login = name
                                    self.avatar_url = None
                                    self.html_url = None
                                    self.contributions = 1
                                    
                            # 使用临时贡献者对象
                            temp_contributor = TempContributor(commit_author.name, commit_author.email)
                            contributor_id = self._save_contributor(temp_contributor)
                            
                            processed_contributors.add(temp_contributor_id)
                            contributors_processed += 1
                        else:
                            # 如果是已存在的贡献者，查找其ID
                            query = "SELECT github_id FROM contributors WHERE github_id = %s"
                            result = self.db_manager.execute_query(query, (temp_contributor_id,))
                            if result:
                                contributor_id = result[0]['github_id']
                    elif contributor:
                        # 如果有GitHub用户信息，使用正常的贡献者处理逻辑
                        if contributor.id not in processed_contributors:
                            contributor_id = self._save_contributor(contributor)
                            processed_contributors.add(contributor.id)
                            contributors_processed += 1
                        else:
                            # 如果是已存在的贡献者，查找其ID
                            query = "SELECT github_id FROM contributors WHERE github_id = %s"
                            result = self.db_manager.execute_query(query, (contributor.id,))
                            if result:
                                contributor_id = result[0]['github_id']
                    
                    # 只有当contributor_id有效时才保存提交记录，避免外键约束错误
                    if contributor_id:
                        self._save_commit(project_id, contributor_id, commit_sha, commit_message, commit_date, 
                                         commit_author.name, commit_author.email)
                        commits_processed += 1
                    else:
                        logger.warning(f"跳过提交 {commit_sha[:7]}，无法找到有效的贡献者ID")
                    
                    # 每处理100个提交检查一次
                    if commits_processed % 100 == 0:
                        logger.debug(f"已处理项目 {repo.full_name} 的 {commits_processed} 个提交记录")
                
                except Exception as e:
                    logger.warning(f"处理提交 {commit.sha} 时出错: {e}")
                    continue
            
            # 更新项目的提交数量统计
            self._update_project_commits_count(project_id, commits_processed)
            
            logger.info(f"从commit数据获取项目 {repo.full_name} 贡献者和提交信息完成，共处理 {contributors_processed} 个贡献者和 {commits_processed} 个提交记录")
            return contributors_processed
            
        except Exception as e:
            logger.error(f"从commit数据获取项目 {repo.full_name} 贡献者和提交信息时出错: {e}")
            return 0
    
    def _update_project_commits_count(self, project_id, commits_count):
        """更新项目的提交数量统计
        
        Args:
            project_id: 项目ID
            commits_count: 提交数量
        """
        try:
            logger.info(f"开始更新项目ID {project_id} 的提交数量统计")
            # 更新statistics表中的提交数量
            query = "UPDATE statistics SET total_commits = %s WHERE project_id = %s"
            self.db_manager.execute_query(query, (commits_count, project_id))
            
            logger.info(f"项目ID {project_id} 的提交数量统计更新完成，共 {commits_count} 个提交")
            
        except Exception as e:
            logger.error(f"更新项目ID {project_id} 的提交数量时出错: {e}")
    
    
    def _save_contributors_from_api(self, repo, project_id):
        """使用GitHub API传统方式获取贡献者信息（备用方法）
        
        Args:
            repo: GitHub仓库对象
            project_id: 项目ID
            
        Returns:
            int: 处理的贡献者数量
        """
        try:
            # 使用类中已定义的since_date
            since_date = self.since_date
            logger.info(f"使用传统API方式获取项目 {repo.full_name} 的2025年以来的贡献者")
            
            # 使用生成器获取贡献者，避免一次性加载所有贡献者
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
            
            return contributors_processed
            
        except Exception as e:
            # 专门处理大型仓库的API限制错误
            error_message = str(e)
            if "contributor list is too large" in error_message or "403" in error_message:
                logger.warning(f"GitHub API限制：无法获取项目 {repo.full_name} 的贡献者列表，这是因为仓库历史或贡献者列表过大")
            else:
                logger.error(f"使用API方式获取项目 {repo.full_name} 贡献者信息时出错: {e}")
            return 0
    
    def _save_commit(self, project_id, contributor_id, sha, message, created_at, author_name, author_email):
        """保存提交记录信息"""
        try:
            # 检查提交是否已存在
            query = "SELECT id FROM commits WHERE project_id = %s AND sha = %s"
            result = self.db_manager.execute_query(query, (project_id, sha))
            
            if result:
                logger.info(f"提交记录 {sha[:7]} 已存在，跳过处理")
                return
            
            # 插入新提交记录
            query = """
            INSERT INTO commits (project_id, contributor_id, sha, message, created_at, author_name, author_email)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            params = (project_id, contributor_id, sha, message, created_at, author_name, author_email)
            self.db_manager.execute_query(query, params)
            
        except Exception as e:
            logger.error(f"保存提交记录 {sha} 时出错: {e}")
    
    
    def _save_contributor(self, contributor):
        """保存贡献者基本信息到数据库
        
        此方法负责将GitHub贡献者信息保存到contributors表中，采用"先检查后插入"的策略，
        避免数据重复。如果贡献者已存在，则直接返回其ID；如果不存在，则插入新记录。
        
        Args:
            contributor: 贡献者对象，包含以下必要属性：
                        - id: GitHub用户ID，用于唯一标识贡献者
                        - login: GitHub用户名
                        - avatar_url: 头像URL
                        - html_url: 用户GitHub主页URL
                        - contributions: 贡献数量
        
        Returns:
            int or None: 成功时返回贡献者在数据库中的ID，失败时返回None
            
        Raises:
            Exception: 当数据库操作失败时抛出异常，但会被方法内部捕获并记录
        """
        try:
            
            # 检查贡献者是否已存在于数据库中
            # 使用GitHub用户ID作为唯一标识进行查询
            query = "SELECT github_id FROM contributors WHERE github_id = %s"
            result = self.db_manager.execute_query(query, (contributor.id,))
            
            # 如果贡献者已存在，直接返回其github_id（现在作为主键）
            if result:
                contributor_id = result[0]['github_id']
                logger.info(f"贡献者 {contributor.login} 已存在，ID: {contributor_id}")
                return contributor_id
            
            # 如果贡献者不存在，通过GitHub API获取更详细的用户信息
            # 这包括电子邮件、位置、公司等非必要但有用的信息
            details = self.github_api.get_contributor_details(contributor.login)
            
            # 构建插入新贡献者记录的SQL查询
            query = """
            INSERT INTO contributors (
                github_id, username, avatar_url, html_url, 
                contributions, email, location, company, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            # 准备SQL查询参数，使用字典get方法安全获取可能不存在的详细信息
            params = (
                contributor.id,              # GitHub用户ID
                contributor.login,           # GitHub用户名
                contributor.avatar_url,      # 用户头像URL
                contributor.html_url,        # 用户GitHub主页URL
                contributor.contributions,   # 对项目的贡献数量
                details.get('email') if details else None,    # 电子邮件（可能不存在）
                details.get('location') if details else None, # 位置信息（可能不存在）
                details.get('company') if details else None,  # 公司信息（可能不存在）
                details.get('created_at') if details else None # 账号创建时间（可能不存在）
            )
            
            # 执行插入操作
            self.db_manager.execute_query(query, params)
            
            # 由于github_id现在是主键，直接使用contributor.id作为返回值
            contributor_id = contributor.id
            
            # 返回新贡献者的ID
            return contributor_id
            
        except Exception as e:
            # 捕获所有异常，记录错误日志
            logger.error(f"保存贡献者 {contributor.login} 时出错: {e}")
            
            # 发生错误时返回None
            return None
    
    def _save_project_contributor(self, project_id, contributor_id, contributions):
        """保存项目-贡献者关联"""
        try:
            logger.info(f"开始处理项目-贡献者关联: 项目ID {project_id}，贡献者ID {contributor_id}")
            query = """
            INSERT INTO project_contributors (project_id, contributor_id, contributions)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE contributions = %s
            """
            
            self.db_manager.execute_query(query, (project_id, contributor_id, contributions, contributions))
            logger.info(f"项目-贡献者关联处理完成: 项目ID {project_id}，贡献者ID {contributor_id}")
            
        except Exception as e:
            logger.error(f"保存项目-贡献者关联时出错: {e}")
    
    # 注意：_save_recent_commits方法已被移除，不再保存提交数据

# 创建数据采集器实例
data_collector = DataCollector()