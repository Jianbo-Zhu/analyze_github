import os
import logging
from datetime import datetime
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm, inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from src.utils.logger import reporting_logger

logger = reporting_logger

class PDFReportGenerator:
    """PDF报告生成器，负责创建格式化的PDF报告"""
    
    def __init__(self):
        """初始化PDF报告生成器"""
        self.styles = self._setup_styles()
    
    def _setup_styles(self):
        """设置PDF文档样式
        
        Returns:
            Stylesheet: 样式表对象
        """
        styles = getSampleStyleSheet()
        
        # 自定义标题样式（添加Custom前缀避免冲突）
        styles.add(ParagraphStyle(
            name='CustomTitle',
            fontSize=24,
            textColor=colors.HexColor('#1890ff'),
            alignment=TA_CENTER,
            spaceAfter=30,
            leading=30
        ))
        
        # 自定义副标题样式
        styles.add(ParagraphStyle(
            name='CustomSubtitle',
            fontSize=14,
            textColor=colors.grey,
            alignment=TA_CENTER,
            spaceAfter=20,
            leading=20
        ))
        
        # 自定义章节标题样式
        styles.add(ParagraphStyle(
            name='CustomSectionTitle',
            fontSize=18,
            textColor=colors.HexColor('#1890ff'),
            alignment=TA_LEFT,
            spaceAfter=15,
            leading=22,
            borderColor=colors.HexColor('#1890ff'),
            borderWidth=0,  # 不使用边框，使用分隔线
            borderBottomWidth=2,
            borderBottomPadding=5
        ))
        
        # 自定义子章节标题样式
        styles.add(ParagraphStyle(
            name='CustomSubsectionTitle',
            fontSize=16,
            textColor=colors.HexColor('#52c41a'),
            alignment=TA_LEFT,
            spaceAfter=10,
            leading=20
        ))
        
        # 自定义正文样式
        styles.add(ParagraphStyle(
            name='CustomBodyText',
            fontSize=12,
            textColor=colors.black,
            alignment=TA_LEFT,
            spaceAfter=10,
            leading=18
        ))
        
        # 自定义列表项样式
        styles.add(ParagraphStyle(
            name='CustomListItem',
            fontSize=12,
            textColor=colors.black,
            alignment=TA_LEFT,
            leftIndent=20,
            spaceAfter=5,
            leading=16
        ))
        
        # 自定义强调样式
        styles.add(ParagraphStyle(
            name='CustomEmphasis',
            fontSize=12,
            textColor=colors.HexColor('#1890ff'),
            alignment=TA_LEFT,
            spaceAfter=10,
            leading=18
        ))
        
        # 自定义页脚样式
        styles.add(ParagraphStyle(
            name='CustomFooter',
            fontSize=10,
            textColor=colors.grey,
            alignment=TA_CENTER,
            leading=12
        ))
        
        return styles
    
    def _add_cover_page(self, story, metadata):
        """添加封面页
        
        Args:
            story: Platypus story对象
            metadata: 元数据字典
        """
        # 添加标题
        story.append(Paragraph('<b>GitHub开源项目分析报告</b>', self.styles['CustomTitle']))
        
        # 添加副标题和元数据
        subtitle_text = (f"{metadata.get('analysis_period', '2025年以来')}<br/>"  
                        f"分析项目数：{metadata.get('project_count', 0)}<br/>"  
                        f"生成时间：{metadata.get('generated_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}")
        story.append(Paragraph(subtitle_text, self.styles['CustomSubtitle']))
        
        # 添加分隔和版权信息
        story.append(Spacer(1, 10*cm))
        
        footer_text = f"© 2025 GitHub开源项目分析系统 | 版本：{metadata.get('version', '1.0.0')}"
        story.append(Paragraph(footer_text, self.styles['CustomFooter']))
        
        # 分页
        story.append(PageBreak())
    
    def _add_summary_section(self, story, summary):
        """添加摘要统计部分
        
        Args:
            story: Platypus story对象
            summary: 摘要数据字典
        """
        story.append(Paragraph('<b>摘要统计</b>', self.styles['CustomSectionTitle']))
        
        # 创建统计数据表格
        data = [
            ['指标', '数值'],
            ['分析项目总数', str(summary.get('total_projects', 0))],
            ['贡献者总数', str(summary.get('total_contributors', 0))],
            ['提交总数', str(summary.get('total_commits', 0))],
            ['中位数星数', str(summary.get('median_stars', 0))],
            ['中位数Forks', str(summary.get('median_forks', 0))],
            ['中位数贡献者数', str(summary.get('median_contributors', 0))]
        ]
        
        table = Table(data, colWidths=[8*cm, 6*cm])
        
        # 设置表格样式
        table_style = TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1890ff')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f0f0f0')),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 11),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#d9d9d9'))
        ])
        
        table.setStyle(table_style)
        story.append(table)
        story.append(Spacer(1, 2*cm))
    
    def _add_language_section(self, story, language_data):
        """添加编程语言分布部分
        
        Args:
            story: Platypus story对象
            language_data: 语言分布数据
        """
        story.append(Paragraph('<b>编程语言分布</b>', self.styles['CustomSectionTitle']))
        
        # 创建语言分布表格
        languages = language_data.get('distribution', {})
        top_languages = sorted(languages.items(), key=lambda x: x[1], reverse=True)[:10]
        
        data = [['编程语言', '占比 (%)']]
        for lang, percentage in top_languages:
            data.append([lang, f"{percentage:.1f}"])
        
        table = Table(data, colWidths=[10*cm, 4*cm])
        
        # 设置表格样式
        table_style = TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1890ff')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f0f0f0')),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 11),
            ('ALIGN', (0, 0), (0, -1), 'LEFT'),
            ('ALIGN', (1, 0), (1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#d9d9d9'))
        ])
        
        table.setStyle(table_style)
        story.append(table)
        
        # 添加洞察
        if top_languages:
            top_lang, top_percent = top_languages[0]
            insight_text = f"<b>洞察：</b>最受欢迎的编程语言是 {top_lang}，占比达到 {top_percent:.1f}%。这反映了当前软件开发领域的技术趋势和偏好。"
            story.append(Spacer(1, 1*cm))
            story.append(Paragraph(insight_text, self.styles['CustomBodyText']))
        
        story.append(Spacer(1, 2*cm))
    
    def _add_contributor_section(self, story, contributor_data):
        """添加贡献者分析部分
        
        Args:
            story: Platypus story对象
            contributor_data: 贡献者数据
        """
        story.append(Paragraph('<b>贡献者分析</b>', self.styles['CustomSectionTitle']))
        
        # 贡献者地域分布
        story.append(Paragraph('<b>贡献者地域分布（前10名）</b>', self.styles['CustomSubsectionTitle']))
        country_data = contributor_data.get('country_distribution', {})
        top_countries = sorted(country_data.items(), key=lambda x: x[1], reverse=True)[:10]
        
        data = [['国家/地区', '贡献者数量']]
        for country, count in top_countries:
            data.append([country, str(count)])
        
        table = Table(data, colWidths=[10*cm, 4*cm])
        
        # 设置表格样式
        table_style = TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#52c41a')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f0f0f0')),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 11),
            ('ALIGN', (0, 0), (0, -1), 'LEFT'),
            ('ALIGN', (1, 0), (1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#d9d9d9'))
        ])
        
        table.setStyle(table_style)
        story.append(table)
        story.append(Spacer(1, 2*cm))
    
    def _add_project_domains_section(self, story, domain_data):
        """添加项目领域分析部分
        
        Args:
            story: Platypus story对象
            domain_data: 项目领域数据
        """
        story.append(Paragraph('<b>项目领域分析</b>', self.styles['CustomSectionTitle']))
        
        # 项目领域分布
        domains = domain_data.get('distribution', {})
        top_domains = sorted(domains.items(), key=lambda x: x[1], reverse=True)[:10]
        
        data = [['项目领域', '项目数量']]
        for domain, count in top_domains:
            data.append([domain, str(count)])
        
        table = Table(data, colWidths=[10*cm, 4*cm])
        
        # 设置表格样式
        table_style = TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#fa8c16')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f0f0f0')),
            ('TEXTCOLOR', (0, 1), (-1, -1), colors.black),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 11),
            ('ALIGN', (0, 0), (0, -1), 'LEFT'),
            ('ALIGN', (1, 0), (1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#d9d9d9'))
        ])
        
        table.setStyle(table_style)
        story.append(table)
        
        # 新兴领域
        emerging_domains = domain_data.get('emerging_domains', [])
        if emerging_domains:
            story.append(Spacer(1, 1*cm))
            emerging_text = f"<b>新兴领域：</b>最具增长潜力的新兴技术领域包括：{', '.join(emerging_domains[:3])}。"
            story.append(Paragraph(emerging_text, self.styles['CustomBodyText']))
        
        story.append(Spacer(1, 2*cm))
    
    def _add_findings_section(self, story, findings):
        """添加有趣发现部分
        
        Args:
            story: Platypus story对象
            findings: 有趣发现列表
        """
        story.append(Paragraph('<b>有趣的发现</b>', self.styles['CustomSectionTitle']))
        
        for i, finding in enumerate(findings, 1):
            finding_text = f"<b>{i}. {finding['title']}</b><br/>{finding['description']}"
            story.append(Paragraph(finding_text, self.styles['CustomBodyText']))
            story.append(Spacer(1, 0.5*cm))
        
        story.append(Spacer(1, 2*cm))
    
    def _add_conclusion_section(self, story, report_data, metadata):
        """添加结论与展望部分
        
        Args:
            story: Platypus story对象
            report_data: 报告数据
            metadata: 元数据
        """
        story.append(Paragraph('<b>结论与展望</b>', self.styles['CustomSectionTitle']))
        
        # 结论
        total_projects = report_data.get('summary', {}).get('total_projects', 0)
        conclusion_text = f"通过对 {total_projects} 个GitHub开源项目的深入分析，我们揭示了当前开源生态系统的关键特征和趋势。"
        story.append(Paragraph(conclusion_text, self.styles['CustomBodyText']))
        
        # 主要发现
        story.append(Spacer(1, 0.5*cm))
        story.append(Paragraph('<b>主要发现</b>', self.styles['CustomSubsectionTitle']))
        
        # 获取主要发现数据
        language_data = report_data.get('language_distribution', {})
        languages = language_data.get('distribution', {})
        top_language = list(languages.keys())[0] if languages else '未知'
        
        contributor_data = report_data.get('contributor_demographics', {})
        country_data = contributor_data.get('country_distribution', {})
        top_country = list(country_data.keys())[0] if country_data else '未知'
        
        domain_data = report_data.get('project_domains', {})
        domains = domain_data.get('distribution', {})
        top_domain = list(domains.keys())[0] if domains else '未知'
        
        avg_response_time = report_data.get('community_health', {}).get('avg_response_time', '未知')
        
        findings = [
            f"编程语言分布显示 {top_language} 占据主导地位，反映了当前技术栈趋势。",
            f"贡献者地域分布呈现全球化特征，{top_country} 是最大的贡献者来源。",
            f"项目领域多样化，其中 {top_domain} 是最热门的技术领域。",
            f"社区活跃度维持在较高水平，平均响应时间为 {avg_response_time} 天。"
        ]
        
        for finding in findings:
            story.append(Paragraph(f"• {finding}", self.styles['CustomListItem']))
        
        # 未来展望
        story.append(Spacer(1, 1*cm))
        story.append(Paragraph('<b>未来展望</b>', self.styles['CustomSubsectionTitle']))
        
        outlook_text = "随着开源运动的持续发展，我们预计以下趋势将在未来进一步强化："
        story.append(Paragraph(outlook_text, self.styles['CustomBodyText']))
        
        outlooks = [
            "跨领域技术融合将加速，特别是在人工智能、云计算和区块链等领域。",
            "开源社区将更加全球化，来自新兴市场的贡献将显著增加。",
            "项目维护和治理模式将进一步完善，提高开源项目的可持续性。",
            "企业参与开源的深度和广度将继续扩大，形成更加繁荣的开源生态系统。"
        ]
        
        for i, outlook in enumerate(outlooks, 1):
            story.append(Paragraph(f"{i}. {outlook}", self.styles['CustomListItem']))
    
    def _add_page_footer(self, canvas, doc):
        """添加页脚
        
        Args:
            canvas: Canvas对象
            doc: Document对象
        """
        canvas.saveState()
        footer_text = f"Page {doc.page} | GitHub开源项目分析报告"
        canvas.setFont('Helvetica', 9)
        canvas.setFillColor(colors.grey)
        canvas.drawCentredString(doc.width/2 + doc.leftMargin, 1.5*cm, footer_text)
        canvas.restoreState()
    
    def generate(self, report_data, metadata, output_path):
        """生成PDF报告
        
        Args:
            report_data: 报告数据
            metadata: 元数据
            output_path: 输出路径
            
        Returns:
            str: 生成的文件路径
        """
        logger.info(f"生成PDF报告：{output_path}")
        
        # 确保输出目录存在
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # 创建PDF文档
        doc = SimpleDocTemplate(
            output_path,
            pagesize=A4,
            rightMargin=2*cm,
            leftMargin=2*cm,
            topMargin=2*cm,
            bottomMargin=2*cm
        )
        
        # 创建Platypus story
        story = []
        
        # 添加封面页
        self._add_cover_page(story, metadata)
        
        # 添加摘要统计部分
        summary = report_data.get('summary', {})
        self._add_summary_section(story, summary)
        
        # 添加编程语言分布部分
        language_data = report_data.get('language_distribution', {})
        self._add_language_section(story, language_data)
        
        # 添加贡献者分析部分
        contributor_data = report_data.get('contributor_demographics', {})
        self._add_contributor_section(story, contributor_data)
        
        # 添加项目领域分析部分
        domain_data = report_data.get('project_domains', {})
        self._add_project_domains_section(story, domain_data)
        
        # 添加有趣发现部分
        findings = report_data.get('interesting_findings', [])
        self._add_findings_section(story, findings)
        
        # 添加结论与展望部分
        self._add_conclusion_section(story, report_data, metadata)
        
        # 构建PDF
        doc.build(story, onFirstPage=self._add_page_footer, onLaterPages=self._add_page_footer)
        
        logger.info(f"PDF报告已保存：{output_path}")
        return output_path