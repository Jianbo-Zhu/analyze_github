import os
import json
import logging
from datetime import datetime
from src.utils.config import config
from src.utils.logger import reporting_logger
from src.reporting.html_report import HTMLReportGenerator

# 尝试导入PDF报告生成器，如果失败则记录但不终止
PDFReportGenerator = None
try:
    from src.reporting.pdf_report import PDFReportGenerator
except ImportError:
    reporting_logger.warning("PDF报告生成功能需要安装reportlab库")

logger = reporting_logger

class ReportGenerator:
    """报告生成器核心类，负责协调整个报告生成流程"""
    
    def __init__(self):
        """初始化报告生成器"""
        self.output_dir = config.OUTPUT_DIR
        self.html_report = HTMLReportGenerator()
        self.pdf_report = PDFReportGenerator() if PDFReportGenerator is not None else None
        self.report_data = {}
        self.report_metadata = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'version': '1.0.0',
            'analysis_period': '2025年以来',
            'project_count': config.MAX_PROJECTS
        }
    
    def load_analysis_results(self, analysis_results):
        """加载分析结果数据
        
        Args:
            analysis_results: 分析结果字典
        """
        logger.info("加载分析结果数据")
        self.report_data = analysis_results
        self.report_metadata['actual_projects_analyzed'] = analysis_results.get('metadata', {}).get('total_projects', 0)
    
    def generate_summary_statistics(self):
        """生成摘要统计信息
        
        Returns:
            dict: 摘要统计信息
        """
        logger.info("生成摘要统计信息")
        
        # 提取关键统计数据
        summary = {
            'total_projects': self.report_data.get('metadata', {}).get('total_projects', 0),
            'total_contributors': self.report_data.get('metadata', {}).get('total_contributors', 0),
            'total_commits': self.report_data.get('metadata', {}).get('total_commits', 0),
            'top_languages': self.report_data.get('language_distribution', {}).get('top_languages', []),
            'top_countries': self.report_data.get('contributor_demographics', {}).get('top_countries', []),
            'top_domains': self.report_data.get('project_domains', {}).get('top_domains', []),
            'median_stars': self.report_data.get('project_metrics', {}).get('median_stars', 0),
            'median_forks': self.report_data.get('project_metrics', {}).get('median_forks', 0),
            'median_contributors': self.report_data.get('project_metrics', {}).get('median_contributors', 0)
        }
        
        # 保存摘要到报告数据中
        self.report_data['summary'] = summary
        return summary
    
    def prepare_chart_data(self):
        """准备图表数据
        
        Returns:
            dict: 格式化的图表数据
        """
        logger.info("准备图表数据")
        
        chart_data = {
            'language_distribution': [],
            'contributor_countries': [],
            'project_domains': [],
            'contributor_activity': [],
            'project_age_distribution': []
        }
        
        # 辅助函数：高效获取排序后的前N个元素
        def get_top_n_items(data_dict, n=15):
            # 使用生成器表达式而不是列表推导式，避免创建大列表
            return list([
                {'name': key, 'value': value}
                for _, key, value in sorted(
                    ((-value, key, value) for key, value in data_dict.items()),
                    key=lambda x: x[0]
                )[:n]
            ])
        
        # 准备编程语言分布图数据
        if 'language_distribution' in self.report_data:
            languages = self.report_data['language_distribution'].get('distribution', {})
            chart_data['language_distribution'] = get_top_n_items(languages)
        
        # 准备贡献者国家分布数据
        if 'contributor_demographics' in self.report_data:
            countries = self.report_data['contributor_demographics'].get('country_distribution', {})
            chart_data['contributor_countries'] = get_top_n_items(countries)
        
        # 准备项目领域分布数据
        if 'project_domains' in self.report_data:
            domains = self.report_data['project_domains'].get('distribution', {})
            chart_data['project_domains'] = get_top_n_items(domains)
        
        # 准备贡献者活跃度数据 - 直接使用生成器表达式
        if 'contributor_activity' in self.report_data:
            activity = self.report_data['contributor_activity'].get('commits_by_period', [])
            chart_data['contributor_activity'] = (
                {'period': period, 'commits': count}
                for period, count in activity
            )
        
        # 准备项目年龄分布数据 - 直接使用生成器表达式
        if 'project_lifecycle' in self.report_data:
            age_dist = self.report_data['project_lifecycle'].get('age_distribution', [])
            chart_data['project_age_distribution'] = (
                {'age_group': group, 'count': count}
                for group, count in age_dist
            )
        
        # 保存图表数据到报告数据中
        self.report_data['chart_data'] = chart_data
        return chart_data
    
    def prepare_interesting_findings(self):
        """准备有趣的发现和洞察
        
        Returns:
            list: 有趣发现的列表
        """
        logger.info("准备有趣的发现和洞察")
        
        findings = []
        
        # 辅助函数：高效获取字典中的最大值项
        def get_max_item(data_dict):
            if not data_dict:
                return None
            # 使用迭代器逐个比较，避免创建整个列表
            max_key = None
            max_value = -float('inf')
            for key, value in data_dict.items():
                if value > max_value:
                    max_value = value
                    max_key = key
            return max_key, max_value
        
        # 基于数据生成洞察
        # 1. 最流行的编程语言
        if 'language_distribution' in self.report_data:
            languages = self.report_data['language_distribution'].get('distribution', {})
            top_lang = get_max_item(languages)
            if top_lang:
                findings.append({
                    'title': '最受欢迎的编程语言',
                    'description': f"{top_lang[0]} 是分析期间最受欢迎的编程语言，占比 {top_lang[1]}%。",
                    'type': 'language'
                })
        
        # 2. 贡献者地域分布
        if 'contributor_demographics' in self.report_data:
            countries = self.report_data['contributor_demographics'].get('country_distribution', {})
            top_country = get_max_item(countries)
            if top_country:
                findings.append({
                    'title': '主要贡献者来源国',
                    'description': f"{top_country[0]} 是最大的贡献者来源国，贡献了约 {top_country[1]}% 的贡献者。",
                    'type': 'geography'
                })
        
        # 3. 项目活跃度指标
        if 'community_health' in self.report_data:
            avg_response_time = self.report_data['community_health'].get('avg_response_time', 0)
            findings.append({
                'title': '社区响应速度',
                'description': f"项目的平均问题响应时间为 {avg_response_time} 天，反映了社区的活跃度和维护质量。",
                'type': 'community'
            })
        
        # 4. 新兴技术领域 - 使用迭代器获取前3个元素
        if 'project_domains' in self.report_data:
            emerging_domains = self.report_data['project_domains'].get('emerging_domains', [])
            if emerging_domains:
                # 生成器表达式获取前3个元素
                top_emerging_domains = (domain for i, domain in enumerate(emerging_domains) if i < 3)
                # 仅在需要时才转换为列表以连接字符串
                findings.append({
                    'title': '新兴技术领域',
                    'description': f"最具增长潜力的新兴技术领域包括：{', '.join(list(top_emerging_domains))}。",
                    'type': 'domain'
                })
        
        # 5. 项目规模洞察
        if 'project_metrics' in self.report_data:
            median_size = self.report_data['project_metrics'].get('median_size', 0)
            findings.append({
                'title': '项目规模特征',
                'description': f"分析的项目中位数大小为 {median_size} KB，显示了当前开源项目的典型规模。",
                'type': 'size'
            })
        
        # 保存洞察到报告数据中
        self.report_data['interesting_findings'] = findings
        return findings
    
    def save_report_data(self, filename='report_data.json'):
        """保存报告数据到JSON文件
        
        Args:
            filename: 文件名
            
        Returns:
            str: 保存的文件路径
        """
        logger.info(f"保存报告数据到 {filename}")
        
        # 确保输出目录存在
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 处理报告数据中的生成器，转换为列表以便JSON序列化
        def process_generator_data(data):
            """递归处理数据中的生成器，转换为列表"""
            if isinstance(data, dict):
                return {key: process_generator_data(value) for key, value in data.items()}
            elif isinstance(data, (list, tuple)):
                return [process_generator_data(item) for item in data]
            elif hasattr(data, '__iter__') and not isinstance(data, (str, bytes, dict, list, tuple, set)):
                # 如果是生成器或其他迭代器，转换为列表
                return list(data)
            return data
        
        # 合并并处理所有数据
        report_data_processed = process_generator_data(self.report_data)
        full_report_data = {
            'metadata': self.report_metadata,
            'data': report_data_processed
        }
        
        # 保存到文件
        file_path = os.path.join(self.output_dir, filename)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(full_report_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"报告数据已保存到 {file_path}")
        return file_path
    
    def generate_html_report(self, filename='github_analysis_report.html'):
        """生成HTML报告
        
        Args:
            filename: 文件名
            
        Returns:
            str: 生成的文件路径
        """
        logger.info("开始生成HTML报告")
        
        # 准备报告数据
        self.generate_summary_statistics()
        self.prepare_chart_data()
        self.prepare_interesting_findings()
        
        # 生成HTML报告
        file_path = self.html_report.generate(
            report_data=self.report_data,
            metadata=self.report_metadata,
            output_path=os.path.join(self.output_dir, filename)
        )
        
        logger.info(f"HTML报告已生成：{file_path}")
        return file_path
    
    def generate_pdf_report(self, filename='github_analysis_report.pdf'):
        """生成PDF报告
        
        Args:
            filename: 文件名
            
        Returns:
            str: 生成的文件路径
        
        Raises:
            ImportError: 当缺少reportlab库时
        """
        logger.info("开始生成PDF报告")
        
        # 检查PDF生成器是否可用
        if PDFReportGenerator is None:
            error_msg = "PDF报告生成功能未可用，请安装reportlab库：pip install reportlab Pillow"
            reporting_logger.error(error_msg)
            raise ImportError(error_msg)
        
        # 确保数据已准备好
        if not self.report_data.get('summary'):
            self.generate_summary_statistics()
        
        # 生成PDF报告
        file_path = self.pdf_report.generate(
            report_data=self.report_data,
            metadata=self.report_metadata,
            output_path=os.path.join(self.output_dir, filename)
        )
        
        logger.info(f"PDF报告已生成：{file_path}")
        return file_path
    
    def generate_all_reports(self):
        """生成所有类型的报告
        
        Returns:
            dict: 包含所有生成文件路径的字典
        """
        logger.info("开始生成所有报告")
        
        # 准备所有报告数据
        self.generate_summary_statistics()
        self.prepare_chart_data()
        self.prepare_interesting_findings()
        
        # 保存报告数据
        data_file = self.save_report_data()
        
        # 生成HTML报告
        html_file = self.generate_html_report()
        
        # 生成PDF报告
        pdf_file = self.generate_pdf_report()
        
        logger.info("所有报告生成完成")
        
        return {
            'data_file': data_file,
            'html_report': html_file,
            'pdf_report': pdf_file
        }

# 创建报告生成器实例
report_generator = ReportGenerator()