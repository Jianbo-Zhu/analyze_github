import pandas as pd
import numpy as np
import logging
import re
from datetime import datetime
from src.utils.logger import data_processing_logger

logger = data_processing_logger

class DataProcessor:
    """数据处理器，用于数据清洗和预处理"""
    
    @staticmethod
    def clean_text(text, max_length=1000):
        """清洗文本数据
        
        Args:
            text: 待清洗的文本
            max_length: 最大长度
            
        Returns:
            str: 清洗后的文本
        """
        if not text:
            return ''
        
        # 转换为字符串
        text = str(text)
        
        # 移除多余的空白字符
        text = re.sub(r'\s+', ' ', text)
        
        # 移除特殊字符（保留基本的标点符号）
        text = re.sub(r'[^\w\s.,!?-]', '', text)
        
        # 截取最大长度
        if len(text) > max_length:
            text = text[:max_length] + '...'
        
        return text.strip()
    
    @staticmethod
    def normalize_location(location):
        """规范化地理位置信息
        
        Args:
            location: 地理位置字符串
            
        Returns:
            str: 规范化后的地理位置
        """
        if not location:
            return None
        
        location = str(location).strip()
        
        # 移除多余的空白
        location = re.sub(r'\s+', ' ', location)
        
        # 处理常见的格式问题
        location = location.replace(',,', ',').replace('  ', ' ')
        
        # 常见的位置标准化映射
        location_mapping = {
            'usa': 'United States',
            'us': 'United States',
            'u.s.a.': 'United States',
            'uk': 'United Kingdom',
            'u.k.': 'United Kingdom',
            'china': 'China',
            'cina': 'China',
            'india': 'India',
            'canada': 'Canada',
            'germany': 'Germany',
            'deutschland': 'Germany',
            'france': 'France',
            'japan': 'Japan',
            'italy': 'Italy',
            'italia': 'Italy',
            'brazil': 'Brazil',
            'brasil': 'Brazil',
            'spain': 'Spain',
            'españa': 'Spain',
            'russia': 'Russia',
            'australia': 'Australia',
            'switzerland': 'Switzerland',
            'swiss': 'Switzerland',
            'netherlands': 'Netherlands',
            'nl': 'Netherlands',
            'belgium': 'Belgium',
            'poland': 'Poland',
            'polska': 'Poland',
            'ukraine': 'Ukraine',
            'sweden': 'Sweden',
            'sverige': 'Sweden',
            'norway': 'Norway',
            'norge': 'Norway',
            'denmark': 'Denmark',
            'danmark': 'Denmark',
            'finland': 'Finland',
            'suomi': 'Finland',
            'singapore': 'Singapore',
            'korea': 'South Korea',
            'south korea': 'South Korea',
            'republic of korea': 'South Korea',
            'mexico': 'Mexico',
            'argentina': 'Argentina',
            'chile': 'Chile',
            'colombia': 'Colombia',
            'peru': 'Peru',
            'south africa': 'South Africa',
            'egypt': 'Egypt',
            'saudi arabia': 'Saudi Arabia',
            'uae': 'United Arab Emirates',
            'united arab emirates': 'United Arab Emirates',
            'israel': 'Israel',
            'iran': 'Iran',
            'pakistan': 'Pakistan',
            'bangladesh': 'Bangladesh',
            'vietnam': 'Vietnam',
            'viet nam': 'Vietnam',
            'thailand': 'Thailand',
            'malaysia': 'Malaysia',
            'philippines': 'Philippines',
            'indonesia': 'Indonesia',
            'turkey': 'Turkey',
            'türkiye': 'Turkey'
        }
        
        # 转换为小写进行匹配
        location_lower = location.lower()
        for key, value in location_mapping.items():
            if key in location_lower:
                # 如果包含国家名，返回标准化的国家名
                return value
            
        # 如果找不到匹配，返回原始位置（可能是城市或其他）
        return location if location else None
    
    @staticmethod
    def extract_country_from_location(location):
        """从位置信息中提取国家
        
        Args:
            location: 地理位置字符串
            
        Returns:
            str: 国家名称
        """
        if not location:
            return None
        
        # 尝试从位置字符串中提取最后一部分作为国家
        parts = [p.strip() for p in location.split(',') if p.strip()]
        if parts:
            # 取最后一部分作为国家
            country_candidate = parts[-1].strip()
            
            # 常见的国家名称列表（简化版）
            common_countries = {
                'United States', 'USA', 'US', 'America',
                'China', 'PRC',
                'India',
                'Canada',
                'Germany', 'Deutschland',
                'United Kingdom', 'UK', 'Great Britain',
                'France',
                'Japan',
                'Italy', 'Italia',
                'Brazil', 'Brasil',
                'Spain', 'España',
                'Australia',
                'Russia',
                'Netherlands', 'Holland',
                'Switzerland', 'Swiss',
                'Belgium',
                'Poland', 'Polska',
                'Sweden', 'Sverige',
                'Norway', 'Norge',
                'Denmark', 'Danmark',
                'Finland', 'Suomi',
                'Singapore',
                'South Korea', 'Korea',
                'Mexico',
                'Turkey', 'Türkiye'
            }
            
            # 检查候选是否在常见国家列表中
            if country_candidate in common_countries:
                return country_candidate
            
            # 检查候选的小写版本
            country_lower = country_candidate.lower()
            for country in common_countries:
                if country_lower == country.lower():
                    return country
        
        # 如果无法识别，返回规范化后的位置
        return DataProcessor.normalize_location(location)
    
    @staticmethod
    def normalize_company(company):
        """规范化公司名称
        
        Args:
            company: 公司名称
            
        Returns:
            str: 规范化后的公司名称
        """
        if not company:
            return None
        
        company = str(company).strip()
        
        # 移除常见的前缀和后缀
        patterns_to_remove = [
            r'^@',  # 移除@符号
            r'^the\s+',  # 移除开头的"The"
            r',\s+inc\.?$',  # 移除结尾的", Inc."
            r',\s+llc\.?$',  # 移除结尾的", LLC."
            r',\s+corp\.?$',  # 移除结尾的", Corp."
            r',\s+gmbh$',  # 移除结尾的", GmbH"
            r',\s+ag$',  # 移除结尾的", AG"
            r'\s+inc\.?$',  # 移除结尾的" Inc."
            r'\s+llc\.?$',  # 移除结尾的" LLC."
            r'\s+corp\.?$',  # 移除结尾的" Corp."
            r'\s+gmbh$',  # 移除结尾的" GmbH"
            r'\s+ag$',  # 移除结尾的" AG"
        ]
        
        for pattern in patterns_to_remove:
            company = re.sub(pattern, '', company, flags=re.IGNORECASE)
        
        # 常见公司名称映射
        company_mapping = {
            'microsoft': 'Microsoft',
            'google': 'Google',
            'facebook': 'Facebook',
            'meta': 'Meta',
            'amazon': 'Amazon',
            'apple': 'Apple',
            'ibm': 'IBM',
            'oracle': 'Oracle',
            'microsoft corporation': 'Microsoft',
            'google llc': 'Google',
            'amazon.com': 'Amazon',
            'apple inc': 'Apple',
            'alibaba': 'Alibaba',
            'tencent': 'Tencent',
            'baidu': 'Baidu',
            'bytedance': 'ByteDance',
            'alibaba group': 'Alibaba',
            'tencent holdings': 'Tencent',
            'netflix': 'Netflix',
            'spotify': 'Spotify',
            'airbnb': 'Airbnb',
            'uber': 'Uber',
            'lyft': 'Lyft',
            'salesforce': 'Salesforce',
            'atlassian': 'Atlassian',
            'docker': 'Docker',
            'kubernetes': 'Kubernetes',
            'apache software foundation': 'Apache',
            'mozilla': 'Mozilla',
            'linux foundation': 'Linux Foundation',
            'red hat': 'Red Hat',
            'vmware': 'VMware',
            'nvidia': 'NVIDIA',
            'intel': 'Intel',
            'amd': 'AMD',
            'qualcomm': 'Qualcomm',
            'samsung': 'Samsung',
            'sony': 'Sony',
            'nintendo': 'Nintendo',
            'tesla': 'Tesla',
            'spacex': 'SpaceX',
            'github': 'GitHub',
            'gitlab': 'GitLab',
            'bitbucket': 'Bitbucket',
            'digitalocean': 'DigitalOcean',
            'aws': 'AWS',
            'azure': 'Microsoft Azure',
            'gcp': 'Google Cloud',
            'google cloud platform': 'Google Cloud',
            'heroku': 'Heroku',
            'slack': 'Slack',
            'discord': 'Discord',
            'twitter': 'Twitter',
            'x corp': 'X (Twitter)',
            'linkedin': 'LinkedIn',
            'reddit': 'Reddit',
            'pinterest': 'Pinterest',
            'shopify': 'Shopify',
            'stripe': 'Stripe',
            'paypal': 'PayPal',
            'square': 'Square',
            'coinbase': 'Coinbase',
            'binance': 'Binance',
            'alphabet': 'Alphabet',
            'alphabet inc': 'Alphabet',
            'meta platform': 'Meta',
            'meta platforms': 'Meta',
            'meta platforms inc': 'Meta'
        }
        
        # 转换为小写进行匹配
        company_lower = company.lower()
        for key, value in company_mapping.items():
            if key == company_lower:
                return value
        
        return company if company else None
    
    @staticmethod
    def parse_datetime(date_string):
        """解析日期时间字符串
        
        Args:
            date_string: 日期时间字符串
            
        Returns:
            datetime: 解析后的日期时间对象
        """
        if not date_string:
            return None
        
        # 尝试多种常见的日期时间格式
        formats = [
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
            '%d/%m/%Y',
            '%m/%d/%Y',
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_string, fmt)
            except ValueError:
                continue
        
        logger.warning(f"无法解析日期时间: {date_string}")
        return None
    
    @staticmethod
    def calculate_time_difference(start_date, end_date):
        """计算两个日期之间的时间差（天）
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            float: 天数差
        """
        if not start_date or not end_date:
            return None
        
        # 确保两个参数都是datetime对象
        if isinstance(start_date, str):
            start_date = DataProcessor.parse_datetime(start_date)
        if isinstance(end_date, str):
            end_date = DataProcessor.parse_datetime(end_date)
        
        if not start_date or not end_date:
            return None
        
        return (end_date - start_date).days
    
    @staticmethod
    def remove_outliers(data, column, method='iqr', threshold=1.5):
        """移除数据中的异常值
        
        Args:
            data: 数据列表或DataFrame
            column: 列名
            method: 异常值检测方法 ('iqr' 或 'zscore')
            threshold: 阈值
            
        Returns:
            DataFrame: 移除异常值后的数据集
        """
        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = data.copy()
        
        if column not in df.columns:
            logger.error(f"列 {column} 不存在于数据中")
            return df
        
        # 移除缺失值
        df_clean = df.dropna(subset=[column])
        
        if method == 'iqr':
            # 使用IQR方法
            Q1 = df_clean[column].quantile(0.25)
            Q3 = df_clean[column].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR
            
        elif method == 'zscore':
            # 使用Z-score方法
            mean = df_clean[column].mean()
            std = df_clean[column].std()
            lower_bound = mean - threshold * std
            upper_bound = mean + threshold * std
            
        else:
            logger.error(f"未知的异常值检测方法: {method}")
            return df
        
        # 过滤异常值
        filtered_df = df_clean[(df_clean[column] >= lower_bound) & (df_clean[column] <= upper_bound)]
        
        logger.info(f"移除异常值: {len(df) - len(filtered_df)} 条记录被移除")
        return filtered_df
    
    @staticmethod
    def normalize_numeric_data(data, column):
        """归一化数值数据
        
        Args:
            data: 数据列表或DataFrame
            column: 列名
            
        Returns:
            DataFrame: 归一化后的数据集
        """
        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = data.copy()
        
        if column not in df.columns:
            logger.error(f"列 {column} 不存在于数据中")
            return df
        
        # 移除缺失值
        df_clean = df.dropna(subset=[column])
        
        # 归一化（min-max scaling）
        min_val = df_clean[column].min()
        max_val = df_clean[column].max()
        
        if max_val > min_val:
            df[f"{column}_normalized"] = (df_clean[column] - min_val) / (max_val - min_val)
        else:
            # 如果所有值都相同，设置为0
            df[f"{column}_normalized"] = 0
        
        return df
    
    @staticmethod
    def aggregate_data_by_time(data, date_column, aggregation_column, freq='M'):
        """按时间聚合数据
        
        Args:
            data: 数据列表或DataFrame
            date_column: 日期列名
            aggregation_column: 聚合列名
            freq: 时间频率 ('D' 天, 'W' 周, 'M' 月, 'Y' 年)
            
        Returns:
            DataFrame: 聚合后的数据集
        """
        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = data.copy()
        
        if date_column not in df.columns:
            logger.error(f"列 {date_column} 不存在于数据中")
            return df
        
        # 确保日期列是datetime类型
        if not pd.api.types.is_datetime64_any_dtype(df[date_column]):
            try:
                df[date_column] = pd.to_datetime(df[date_column])
            except Exception as e:
                logger.error(f"无法转换 {date_column} 为日期时间: {e}")
                return df
        
        # 按时间聚合
        aggregated = df.resample(freq, on=date_column)[aggregation_column].count().reset_index()
        return aggregated

# 创建数据处理器实例
data_processor = DataProcessor()