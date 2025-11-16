import os
import sys
from datetime import datetime

# 添加src目录到Python路径
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# 导入项目的日志记录器
from src.utils.logger import reporting_logger
from src.reporting.report_generator import ReportGenerator
from src.reporting.html_report import HTMLReportGenerator

# 直接使用预定义的报告日志记录器

# 生成模拟分析结果
def generate_mock_data():
    print("生成模拟数据...")
    return {
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

def main():
    print("===== GitHub开源项目分析系统 - 报告生成测试 =====")
    
    # 确保输出目录存在
    output_dir = os.path.join(os.path.dirname(__file__), 'output')
    os.makedirs(output_dir, exist_ok=True)
    print(f"输出目录: {output_dir}")
    
    # 生成模拟数据
    mock_data = generate_mock_data()
    
    # 创建报告生成器
    report_generator = ReportGenerator()
    report_generator.output_dir = output_dir
    
    # 加载数据
    report_generator.load_analysis_results(mock_data)
    
    # 生成HTML报告
    print("\n生成HTML报告...")
    try:
        html_file = report_generator.generate_html_report('github_analysis_test.html')
        print(f"HTML报告生成成功: {html_file}")
    except Exception as e:
        print(f"HTML报告生成失败: {e}")
    
    # 注：PDF报告生成功能需要安装reportlab库
    # 如需测试PDF报告，请先安装依赖：pip install reportlab Pillow
    
    print("\n===== 测试完成 =====")

if __name__ == "__main__":
    main()