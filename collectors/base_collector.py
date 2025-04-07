#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import time
import random
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

from utils.browser_emulator import BrowserEmulator
from utils.proxy_manager import ProxyManager
from parsers.dataset_extractor import DatasetExtractor
from utils.nlp_tools import is_neuroscience_related, extract_keywords

logger = logging.getLogger(__name__)


class BaseCollector(ABC):
    """所有期刊收集器的基类"""

    def __init__(self, config):
        self.config = config
        self.proxy_manager = ProxyManager(config.get('proxy', {}))
        self.browser = BrowserEmulator()
        self.dataset_extractor = DatasetExtractor()

        # 判断是否是首次运行
        self.is_first_run = True

    def _get_time_range(self, days=None):
        """
        获取时间范围

        Args:
            days (int, optional): 向前搜索的天数，默认使用配置中的值

        Returns:
            tuple: (start_date, end_date)
        """
        end_date = datetime.now()

        if days is None:
            # 使用配置中的天数或默认值
            days = self.config.get('days_to_crawl', 30)

        # 第一次运行爬取过去指定天数的数据，后续只爬取当天的数据
        if self.is_first_run:
            start_date = end_date - timedelta(days=days)
            self.is_first_run = False
        else:
            start_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

        return start_date, end_date

    def _format_date(self, date, format_str="%Y-%m-%d"):
        """格式化日期"""
        return date.strftime(format_str)

    def _random_delay(self, min_seconds=1, max_seconds=3):
        """随机延迟，避免请求过于频繁"""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)
        return delay

    def _long_delay(self):
        """较长延迟，用于周期性暂停或处理完一批数据后"""
        delay = random.uniform(5, 15)
        time.sleep(delay)
        return delay

    @abstractmethod
    def search_articles(self, start_date, end_date, **kwargs):
        """
        搜索文章

        Args:
            start_date (datetime): 起始日期
            end_date (datetime): 结束日期
            **kwargs: 其他搜索参数

        Returns:
            list: 文章列表
        """
        pass

    @abstractmethod
    def get_article_details(self, article):
        """
        获取文章详情

        Args:
            article (dict): 基本文章信息

        Returns:
            dict: 更新后的文章信息
        """
        pass

    def extract_datasets(self, article):
        """
        从文章中提取数据集

        Args:
            article (dict): 文章信息

        Returns:
            list: 数据集列表
        """
        if 'datasets' in article and article['datasets']:
            return article['datasets']

        # 如果文章中没有预先提取的数据集，尝试提取
        journal_type = article.get('source', 'unknown')

        if 'html_content' in article and article['html_content']:
            # 如果有缓存的HTML内容，直接使用
            html_content = article['html_content']
        else:
            # 否则请求文章页面
            html_content = self.browser.get_page(
                article['url'],
                use_selenium=self.config.get('browser_emulation', True)
            )

        # 使用数据集提取器提取数据集
        datasets = self.dataset_extractor.extract_from_html(
            html_content,
            article['url'],
            journal_type
        )

        # 如果没有找到数据集，检查是否有补充材料
        if not datasets and article.get('supplementary_url'):
            datasets.append({
                'name': "Supplementary Materials",
                'url': article['supplementary_url'],
                'repository': 'journal_supplementary',
                'source': journal_type
            })

        # 为每个数据集添加文章信息
        for dataset in datasets:
            dataset['paper_title'] = article.get('title', 'Unknown')
            dataset['paper_url'] = article['url']
            dataset['paper_doi'] = article.get('doi')
            dataset['extracted_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # 识别数据集类型
            if 'abstract' in article and article['abstract']:
                dataset['data_types'] = self.dataset_extractor.identify_data_types(article['abstract'])

        return datasets

    def collect_papers(self, days=None):
        """
        收集符合条件的论文

        Args:
            days (int, optional): 向前搜索的天数

        Returns:
            list: 收集到的论文列表
        """
        # 获取时间范围
        start_date, end_date = self._get_time_range(days)
        source_name = self.__class__.__name__.replace('Collector', '')

        logger.info(
            f"正在从{source_name}收集{start_date.strftime('%Y-%m-%d')}到{end_date.strftime('%Y-%m-%d')}之间的神经科学论文")

        # 搜索文章
        articles = self.search_articles(start_date, end_date)
        logger.info(f"搜索到 {len(articles)} 篇文章")

        # 收集结果
        all_papers = []

        # 获取每篇文章的详细信息
        for i, article in enumerate(articles):
            try:
                logger.info(f"处理第 {i + 1}/{len(articles)} 篇文章: {article.get('title', 'Unknown')}")

                # 获取文章详情
                article = self.get_article_details(article)

                # 判断是否与神经科学相关
                if article.get('abstract') and is_neuroscience_related(article['abstract']):
                    # 提取数据集信息
                    datasets = self.extract_datasets(article)

                    # 如果找到数据集，添加到文章中并收集
                    if datasets:
                        article['datasets'] = datasets
                        all_papers.append(article)
                        logger.info(f"发现包含数据集的论文: {article['title']}, 数据集数量: {len(datasets)}")
                    else:
                        logger.info(f"未在论文中找到数据集: {article['title']}")
                else:
                    logger.info(f"论文可能与神经科学无关，跳过: {article['title']}")

                # 随机延迟，避免频繁请求
                self._random_delay()

                # 每处理10篇文章休息一下
                if (i + 1) % 10 == 0:
                    logger.info(f"已处理 {i + 1} 篇文章，暂停一下...")
                    self._long_delay()

            except Exception as e:
                logger.error(f"处理文章时出错: {e}, url: {article.get('url', 'Unknown')}")

        logger.info(f"从{source_name}收集到 {len(all_papers)} 篇包含数据集的论文")
        return all_papers

    def save_html_cache(self, url, html_content):
        """
        保存HTML缓存

        Args:
            url (str): 页面URL
            html_content (str): HTML内容
        """
        if not self.config.get('output', {}).get('save_html', False):
            return

        import os
        import hashlib

        # 创建缓存目录
        cache_dir = self.config.get('output', {}).get('html_dir', 'html_cache')
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

        # 生成文件名
        url_hash = hashlib.md5(url.encode()).hexdigest()
        filename = os.path.join(cache_dir, f"{url_hash}.html")

        # 保存HTML内容
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(html_content)
        except Exception as e:
            logger.error(f"保存HTML缓存失败: {e}, url: {url}")