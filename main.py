import os
import time
import logging
from datetime import datetime
from src.utils.config import config
from src.utils.logger import error_logger, reporting_logger as logger
from src.utils.database import db_manager
from src.data_collection.data_collector import data_collector
from src.data_processing.data_analyzer import data_analyzer
from src.reporting.report_generator import report_generator

def setup_environment():
    """设置运行环境"""
    error_logger.info("设置运行环境")
    
    # 确保输出目录存在
    output_dir = config.OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)
    error_logger.info(f"输出目录: {output_dir}")
    
    # 连接数据库
    error_logger.info("连接数据库")
    try:
        db_manager.connect()
        error_logger.info("数据库连接成功")
    except Exception as e:
        error_logger.error(f"数据库连接失败: {e}")
        raise

def collect_data():
    """采集GitHub项目数据"""
    error_logger.info("开始采集GitHub项目数据")
    start_time = time.time()
    
    try:
        # 执行数据采集
        project_count = data_collector.collect_projects()
        error_logger.info(f"数据采集完成，成功采集 {project_count} 个项目")
        
        # 统计采集结果
        total_time = time.time() - start_time
        error_logger.info(f"数据采集耗时: {total_time:.2f} 秒")
        
        return project_count
    except Exception as e:
        error_logger.error(f"数据采集过程中发生错误: {e}")
        raise

def analyze_data():
    """分析采集的数据"""
    error_logger.info("开始分析GitHub项目数据")
    start_time = time.time()
    
    try:
        # 执行数据分析
        analysis_results = data_analyzer.analyze()
        error_logger.info("数据分析完成")
        
        # 统计分析结果
        total_time = time.time() - start_time
        error_logger.info(f"数据分析耗时: {total_time:.2f} 秒")
        
        return analysis_results
    except Exception as e:
        error_logger.error(f"数据分析过程中发生错误: {e}")
        raise

def generate_reports(analysis_results):
    """生成报告"""
    error_logger.info("开始生成报告")
    start_time = time.time()
    
    try:
        # 加载分析结果
        report_generator.load_analysis_results(analysis_results)
        
        # 生成所有报告
        report_files = report_generator.generate_all_reports()
        
        # 统计报告生成结果
        total_time = time.time() - start_time
        error_logger.info(f"报告生成耗时: {total_time:.2f} 秒")
        
        # 输出报告文件路径
        for report_type, file_path in report_files.items():
            error_logger.info(f"{report_type}: {file_path}")
        
        return report_files
    except Exception as e:
        error_logger.error(f"报告生成过程中发生错误: {e}")
        raise

def cleanup():
    """清理资源"""
    error_logger.info("清理资源")
    
    # 关闭数据库连接
    try:
        db_manager.disconnect()
        error_logger.info("数据库连接已关闭")
    except Exception as e:
        error_logger.error(f"关闭数据库连接时发生错误: {e}")

def main():
    """主函数"""
    error_logger.info("===== GitHub开源项目分析系统启动 =====")
    
    start_time = time.time()
    
    try:
        # 1. 设置环境
        setup_environment()
        
        # 2. 采集数据
        collect_data()
        
        # 3. 分析数据
        analysis_results = analyze_data()
        
        # 4. 生成报告
        report_files = generate_reports(analysis_results)
        
        # 5. 清理资源
        cleanup()
        
        total_time = time.time() - start_time
        error_logger.info(f"总执行时间: {total_time:.2f} 秒")
        error_logger.info("===== GitHub开源项目分析系统执行完成 =====")
        
        # 返回生成的报告文件
        return report_files
        
    except KeyboardInterrupt:
        error_logger.warning("程序被用户中断")
        cleanup()
        raise
    except Exception as e:
        error_logger.error(f"程序执行过程中发生错误: {e}")
        cleanup()
        raise

def run_demo_mode():
    """演示模式，使用模拟数据生成报告"""
    error_logger.info("===== GitHub开源项目分析系统（演示模式）启动 =====")
    
    start_time = time.time()
    
    try:
        # 1. 设置环境
        setup_environment()
        
        # 2. 生成模拟分析结果
        analysis_results = {
            'metadata': {
                'total_projects': 5000,
                'total_contributors': 25000,
                'total_commits': 125000,
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            },
            'language_distribution': {
                'distribution': {
                    'JavaScript': 35.2,
                    'Python': 28.7,
                    'Java': 15.3,
                    'TypeScript': 10.8,
                    'Go': 7.5,
                    'C++': 6.2,
                    'Ruby': 4.1,
                    'PHP': 3.9,
                    'C#': 3.5,
                    'Rust': 2.8
                },
                'top_languages': ['JavaScript', 'Python', 'Java', 'TypeScript', 'Go']
            },
            'contributor_demographics': {
                'country_distribution': {
                    'United States': 28.5,
                    'China': 16.3,
                    'India': 14.2,
                    'Germany': 8.7,
                    'United Kingdom': 7.2,
                    'Canada': 5.8,
                    'France': 4.5,
                    'Brazil': 3.9,
                    'Japan': 3.1,
                    'Australia': 2.8
                },
                'top_countries': ['United States', 'China', 'India', 'Germany', 'United Kingdom']
            },
            'project_domains': {
                'distribution': {
                    'Web Development': 28.7,
                    'Data Science & ML': 18.5,
                    'DevOps & SRE': 15.2,
                    'Mobile Development': 12.8,
                    'Backend Systems': 10.5,
                    'Security': 8.3,
                    'Game Development': 6.2,
                    'IoT': 4.8,
                    'Blockchain': 3.2,
                    'AR/VR': 2.6
                },
                'top_domains': ['Web Development', 'Data Science & ML', 'DevOps & SRE', 'Mobile Development', 'Backend Systems'],
                'emerging_domains': ['Machine Learning', 'Blockchain', 'IoT', 'AR/VR']
            },
            'project_metrics': {
                'median_stars': 1250,
                'median_forks': 320,
                'median_contributors': 45,
                'median_size': 1520
            },
            'contributor_activity': {
                'commits_by_period': [
                    ('2025-01', 15000),
                    ('2025-02', 16500),
                    ('2025-03', 18200),
                    ('2025-04', 17800),
                    ('2025-05', 19500),
                    ('2025-06', 20100),
                    ('2025-07', 22300),
                    ('2025-08', 21800)
                ]
            },
            'project_lifecycle': {
                'age_distribution': [
                    ('< 1年', 1500),
                    ('1-2年', 1200),
                    ('2-3年', 1000),
                    ('3-4年', 800),
                    ('4-5年', 500)
                ]
            },
            'community_health': {
                'avg_response_time': 2.3,
                'active_maintainers_per_project': 3.5,
                'issue_resolution_rate': 85.7
            }
        }
        
        # 3. 生成报告
        report_files = generate_reports(analysis_results)
        
        # 4. 清理资源
        cleanup()
        
        total_time = time.time() - start_time
        error_logger.info(f"演示模式总执行时间: {total_time:.2f} 秒")
        error_logger.info("===== GitHub开源项目分析系统（演示模式）执行完成 =====")
        
        # 返回生成的报告文件
        return report_files
        
    except KeyboardInterrupt:
        error_logger.warning("演示程序被用户中断")
        cleanup()
        raise
    except Exception as e:
        error_logger.error(f"演示程序执行过程中发生错误: {e}")
        cleanup()
        raise

if __name__ == "__main__":
    try:
        # 检查是否运行在演示模式
        demo_mode = os.environ.get('DEMO_MODE', 'false').lower() == 'true'
        
        if demo_mode:
            error_logger.info("运行演示模式")
            run_demo_mode()
        else:
            main()
    except Exception as e:
        error_logger.critical(f"程序执行失败: {e}")
        exit(1)