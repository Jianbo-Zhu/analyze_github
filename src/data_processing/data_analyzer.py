import pandas as pd
import numpy as np
import logging
from collections import Counter
from datetime import datetime, timedelta
from src.utils.database import db_manager
from src.utils.logger import data_processing_logger
from src.utils.config import config

logger = data_processing_logger

class DataAnalyzer:
    """GitHub数据分析师"""
    
    def __init__(self):
        self.db_manager = db_manager
    
    def initialize_analysis(self):
        """初始化数据分析过程"""
        logger.info("开始初始化数据分析...")
        
        # 连接数据库
        try:
            self.db_manager.connect()
            logger.info("数据库连接成功")
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            raise
        
        logger.info("数据分析初始化完成")
    
    def analyze_programming_languages(self):
        """分析编程语言分布
        
        Returns:
            dict: 包含编程语言分析结果的数据
        """
        logger.info("开始分析编程语言分布...")
        
        try:
            # 获取所有项目的语言信息
            query = """
            SELECT l.language_name, COUNT(DISTINCT l.project_id) as project_count,
                   SUM(l.percentage) as total_percentage
            FROM languages l
            GROUP BY l.language_name
            ORDER BY project_count DESC
            """
            
            language_data = self.db_manager.execute_query(query)
            df = pd.DataFrame(language_data)
            
            # 计算百分比
            total_projects = df['project_count'].sum()
            df['percentage'] = (df['project_count'] / total_projects) * 100
            
            # 获取主要语言的项目数量
            top_languages = df.head(10).to_dict('records')
            
            # 获取各语言的平均星标数
            query = """
            SELECT l.language_name, AVG(p.stargazers_count) as avg_stars
            FROM languages l
            JOIN projects p ON l.project_id = p.id
            GROUP BY l.language_name
            ORDER BY avg_stars DESC
            LIMIT 10
            """
            
            stars_data = self.db_manager.execute_query(query)
            
            # 获取各语言的平均Fork数
            query = """
            SELECT l.language_name, AVG(p.forks_count) as avg_forks
            FROM languages l
            JOIN projects p ON l.project_id = p.id
            GROUP BY l.language_name
            ORDER BY avg_forks DESC
            LIMIT 10
            """
            
            forks_data = self.db_manager.execute_query(query)
            
            result = {
                'top_languages': top_languages,
                'languages_by_stars': stars_data,
                'languages_by_forks': forks_data,
                'total_projects': total_projects,
                'total_languages': len(df)
            }
            
            logger.info(f"编程语言分析完成，发现 {len(df)} 种不同的编程语言")
            return result
            
        except Exception as e:
            logger.error(f"分析编程语言时出错: {e}")
            raise
    
    def analyze_contributors(self):
        """分析贡献者画像
        
        Returns:
            dict: 包含贡献者分析结果的数据
        """
        logger.info("开始分析贡献者画像...")
        
        try:
            # 获取贡献者地域分布
            query = """
            SELECT location, COUNT(*) as contributor_count
            FROM contributors
            WHERE location IS NOT NULL AND location != ''
            GROUP BY location
            ORDER BY contributor_count DESC
            LIMIT 20
            """
            
            location_data = self.db_manager.execute_query(query)
            
            # 获取贡献者公司分布
            query = """
            SELECT company, COUNT(*) as contributor_count
            FROM contributors
            WHERE company IS NOT NULL AND company != ''
            GROUP BY company
            ORDER BY contributor_count DESC
            LIMIT 20
            """
            
            company_data = self.db_manager.execute_query(query)
            
            # 分析贡献者活跃度
            query = """
            SELECT pc.contributor_id, c.username, SUM(pc.contributions) as total_contributions,
                   COUNT(DISTINCT pc.project_id) as project_count
            FROM project_contributors pc
            JOIN contributors c ON pc.contributor_id = c.id
            GROUP BY pc.contributor_id, c.username
            ORDER BY total_contributions DESC
            LIMIT 20
            """
            
            active_contributors = self.db_manager.execute_query(query)
            
            # 估算贡献者年龄（基于账号创建时间）
            query = """
            SELECT YEAR(created_at) as create_year, COUNT(*) as contributor_count
            FROM contributors
            WHERE created_at IS NOT NULL
            GROUP BY create_year
            ORDER BY create_year
            """
            
            age_data = self.db_manager.execute_query(query)
            
            # 分析贡献者创建时间分布
            current_year = datetime.now().year
            account_ages = []
            for item in age_data:
                if item['create_year']:
                    account_age = current_year - item['create_year'] + 1
                    account_ages.append({
                        'age_group': f"{account_age}年",
                        'count': item['contributor_count']
                    })
            
            # 贡献者性别推测（基于用户名，简单规则）
            query = """
            SELECT username
            FROM contributors
            LIMIT 1000
            """
            
            usernames = self.db_manager.execute_query(query)
            gender_distribution = self._estimate_gender(usernames)
            
            result = {
                'location_distribution': location_data,
                'company_distribution': company_data,
                'top_contributors': active_contributors,
                'account_age_distribution': account_ages,
                'gender_distribution': gender_distribution
            }
            
            logger.info("贡献者画像分析完成")
            return result
            
        except Exception as e:
            logger.error(f"分析贡献者画像时出错: {e}")
            raise
    
    def _estimate_gender(self, usernames):
        """基于用户名简单估算性别分布
        
        Args:
            usernames: 用户名列表
            
        Returns:
            dict: 性别分布估计
        """
        # 简单的性别名字模式（仅供示例，实际应用中可以使用更复杂的算法或API）
        female_patterns = ['anna', 'emma', 'sarah', 'jessica', 'maria', 'lisa', 'nina']
        male_patterns = ['john', 'alex', 'michael', 'david', 'james', 'robert', 'tom']
        
        male_count = 0
        female_count = 0
        unknown_count = 0
        
        for item in usernames:
            username = item['username'].lower()
            is_female = any(pattern in username for pattern in female_patterns)
            is_male = any(pattern in username for pattern in male_patterns)
            
            if is_female and not is_male:
                female_count += 1
            elif is_male and not is_female:
                male_count += 1
            else:
                unknown_count += 1
        
        return [
            {'gender': '男性', 'count': male_count},
            {'gender': '女性', 'count': female_count},
            {'gender': '未知', 'count': unknown_count}
        ]
    
    def analyze_project_domains(self):
        """分析项目领域分类
        
        Returns:
            dict: 包含项目领域分析结果的数据
        """
        logger.info("开始分析项目领域分类...")
        
        try:
            # 基于主题标签分析领域分布
            query = """
            SELECT t.topic_name, COUNT(*) as project_count
            FROM topics t
            GROUP BY t.topic_name
            ORDER BY project_count DESC
            LIMIT 30
            """
            
            topic_data = self.db_manager.execute_query(query)
            
            # 基于语言分析领域趋势
            query = """
            SELECT l.language_name, COUNT(*) as project_count
            FROM languages l
            JOIN projects p ON l.project_id = p.id
            GROUP BY l.language_name
            ORDER BY project_count DESC
            LIMIT 15
            """
            
            language_trends = self.db_manager.execute_query(query)
            
            # 分析项目许可证分布
            query = """
            SELECT license_name, COUNT(*) as project_count
            FROM projects
            WHERE license_name IS NOT NULL AND license_name != ''
            GROUP BY license_name
            ORDER BY project_count DESC
            LIMIT 15
            """
            
            license_data = self.db_manager.execute_query(query)
            
            # 基于描述的领域分类（简化版）
            query = """
            SELECT description
            FROM projects
            WHERE description IS NOT NULL AND description != ''
            """
            
            descriptions = self.db_manager.execute_query(query)
            domain_keywords = self._extract_domain_keywords(descriptions)
            
            result = {
                'top_topics': topic_data,
                'language_trends': language_trends,
                'license_distribution': license_data,
                'domain_keywords': domain_keywords
            }
            
            logger.info("项目领域分类分析完成")
            return result
            
        except Exception as e:
            logger.error(f"分析项目领域分类时出错: {e}")
            raise
    
    def _extract_domain_keywords(self, descriptions):
        """从项目描述中提取领域关键词
        
        Args:
            descriptions: 项目描述列表
            
        Returns:
            list: 关键词分布
        """
        # 预定义的领域关键词（简化版）
        domain_keywords = {
            'web': ['web', 'website', 'frontend', 'backend', 'server', 'client'],
            'mobile': ['mobile', 'android', 'ios', 'phone', 'tablet', 'app'],
            'machine learning': ['ml', 'ai', 'machine learning', 'deep learning', 'neural', 'nlp'],
            'data science': ['data', 'analytics', 'visualization', 'statistics', 'big data'],
            'devops': ['devops', 'docker', 'kubernetes', 'ci/cd', 'automation', 'cloud'],
            'security': ['security', 'cryptography', 'auth', 'authentication', 'encryption'],
            'game': ['game', 'gaming', 'unity', 'unreal', '3d', 'graphics'],
            'tools': ['tool', 'utility', 'cli', 'command line', 'editor', 'ide']
        }
        
        domain_counts = {domain: 0 for domain in domain_keywords.keys()}
        
        for item in descriptions:
            if item['description']:
                desc_lower = item['description'].lower()
                for domain, keywords in domain_keywords.items():
                    for keyword in keywords:
                        if keyword in desc_lower:
                            domain_counts[domain] += 1
                            break  # 每个描述对每个领域只计数一次
        
        # 转换为排序后的列表
        sorted_domains = sorted(
            [{'domain': domain, 'count': count} for domain, count in domain_counts.items()],
            key=lambda x: x['count'],
            reverse=True
        )
        
        return sorted_domains
    
    def analyze_project_lifecycle(self):
        """分析项目生命周期
        
        Returns:
            dict: 包含项目生命周期分析结果的数据
        """
        logger.info("开始分析项目生命周期...")
        
        try:
            # 分析项目创建时间分布
            query = """
            SELECT DATE_FORMAT(created_at, '%Y-%m') as month, COUNT(*) as project_count
            FROM projects
            GROUP BY month
            ORDER BY month
            """
            
            creation_data = self.db_manager.execute_query(query)
            
            # 分析项目更新活跃度
            query = """
            SELECT DATE_FORMAT(updated_at, '%Y-%m') as month, COUNT(*) as updated_count
            FROM projects
            GROUP BY month
            ORDER BY month
            """
            
            update_data = self.db_manager.execute_query(query)
            
            # 分析项目年龄分布
            current_date = datetime.now()
            query = """
            SELECT created_at
            FROM projects
            WHERE created_at IS NOT NULL
            """
            
            all_projects = self.db_manager.execute_query(query)
            age_distribution = self._calculate_project_age_distribution(all_projects, current_date)
            
            # 分析项目活跃度（基于提交次数）
            query = """
            SELECT 
                CASE 
                    WHEN s.total_commits > 500 THEN '非常活跃'
                    WHEN s.total_commits > 200 THEN '活跃'
                    WHEN s.total_commits > 50 THEN '一般'
                    ELSE '低活跃'
                END as activity_level,
                COUNT(*) as project_count
            FROM statistics s
            GROUP BY activity_level
            """
            
            activity_data = self.db_manager.execute_query(query)
            
            result = {
                'creation_trend': creation_data,
                'update_trend': update_data,
                'age_distribution': age_distribution,
                'activity_levels': activity_data
            }
            
            logger.info("项目生命周期分析完成")
            return result
            
        except Exception as e:
            logger.error(f"分析项目生命周期时出错: {e}")
            raise
    
    def _calculate_project_age_distribution(self, projects, current_date):
        """计算项目年龄分布
        
        Args:
            projects: 项目列表
            current_date: 当前日期
            
        Returns:
            list: 年龄分布
        """
        age_ranges = {
            '0-1年': 0,
            '1-2年': 0,
            '2-3年': 0,
            '3-5年': 0,
            '5年以上': 0
        }
        
        for project in projects:
            if project['created_at']:
                created_date = project['created_at']
                if isinstance(created_date, str):
                    created_date = datetime.strptime(created_date, '%Y-%m-%d %H:%M:%S')
                
                age_days = (current_date - created_date).days
                age_years = age_days / 365.25
                
                if age_years < 1:
                    age_ranges['0-1年'] += 1
                elif age_years < 2:
                    age_ranges['1-2年'] += 1
                elif age_years < 3:
                    age_ranges['2-3年'] += 1
                elif age_years < 5:
                    age_ranges['3-5年'] += 1
                else:
                    age_ranges['5年以上'] += 1
        
        return [{'range': age_range, 'count': count} for age_range, count in age_ranges.items()]
    
    def analyze_community_health(self):
        """分析社区健康度
        
        Returns:
            dict: 包含社区健康度分析结果的数据
        """
        logger.info("开始分析社区健康度...")
        
        try:
            # 分析项目贡献者数量分布
            query = """
            SELECT 
                CASE 
                    WHEN contributors_count > 100 THEN '100+ 贡献者'
                    WHEN contributors_count > 50 THEN '50-100 贡献者'
                    WHEN contributors_count > 20 THEN '20-50 贡献者'
                    WHEN contributors_count > 10 THEN '10-20 贡献者'
                    ELSE '少于10贡献者'
                END as contributor_range,
                COUNT(*) as project_count
            FROM projects
            GROUP BY contributor_range
            """
            
            contributor_dist = self.db_manager.execute_query(query)
            
            # 分析星标数与Fork数的相关性
            query = """
            SELECT stargazers_count, forks_count
            FROM projects
            """
            
            stars_forks_data = self.db_manager.execute_query(query)
            correlation = self._calculate_correlation(stars_forks_data, 'stargazers_count', 'forks_count')
            
            # 分析项目讨论活跃度
            query = """
            SELECT 
                CASE 
                    WHEN open_issues_count > 1000 THEN '1000+ issues'
                    WHEN open_issues_count > 500 THEN '500-1000 issues'
                    WHEN open_issues_count > 100 THEN '100-500 issues'
                    WHEN open_issues_count > 50 THEN '50-100 issues'
                    ELSE '少于50 issues'
                END as issues_range,
                COUNT(*) as project_count
            FROM projects
            GROUP BY issues_range
            """
            
            issues_dist = self.db_manager.execute_query(query)
            
            result = {
                'contributor_distribution': contributor_dist,
                'stars_forks_correlation': correlation,
                'issues_distribution': issues_dist
            }
            
            logger.info("社区健康度分析完成")
            return result
            
        except Exception as e:
            logger.error(f"分析社区健康度时出错: {e}")
            raise
    
    def _calculate_correlation(self, data, column1, column2):
        """计算两个列之间的相关性
        
        Args:
            data: 数据列表
            column1: 第一列名称
            column2: 第二列名称
            
        Returns:
            float: 相关系数
        """
        if not data:
            return 0
        
        df = pd.DataFrame(data)
        # 计算皮尔逊相关系数
        correlation = df[column1].corr(df[column2])
        return float(correlation) if not np.isnan(correlation) else 0
    
    def generate_analysis_summary(self):
        """生成综合分析结果
        
        Returns:
            dict: 包含所有分析结果的综合数据
        """
        logger.info("开始生成综合分析结果...")
        
        try:
            # 执行所有分析
            languages_analysis = self.analyze_programming_languages()
            contributors_analysis = self.analyze_contributors()
            domains_analysis = self.analyze_project_domains()
            lifecycle_analysis = self.analyze_project_lifecycle()
            health_analysis = self.analyze_community_health()
            
            # 获取项目总数
            query = "SELECT COUNT(*) as total_count FROM projects"
            total_projects = self.db_manager.execute_query(query)[0]['total_count']
            
            # 获取贡献者总数
            query = "SELECT COUNT(*) as total_count FROM contributors"
            total_contributors = self.db_manager.execute_query(query)[0]['total_count']
            
            # 获取总提交数
            query = "SELECT SUM(total_commits) as total_count FROM statistics"
            total_commits = self.db_manager.execute_query(query)[0]['total_count'] or 0
            
            # 综合结果
            summary = {
                'metadata': {
                    'total_projects': total_projects,
                    'total_contributors': total_contributors,
                    'total_commits': total_commits,
                    'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                },
                'languages': languages_analysis,
                'contributors': contributors_analysis,
                'domains': domains_analysis,
                'lifecycle': lifecycle_analysis,
                'community_health': health_analysis
            }
            
            logger.info(f"综合分析完成: {total_projects} 个项目, {total_contributors} 个贡献者, {total_commits} 次提交")
            return summary
            
        except Exception as e:
            logger.error(f"生成综合分析结果时出错: {e}")
            raise
        finally:
            self.db_manager.disconnect()

# 创建数据分析器实例
data_analyzer = DataAnalyzer()