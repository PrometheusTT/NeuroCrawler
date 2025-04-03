#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import time
import re
import random
from datetime import datetime, timedelta
import requests
import feedparser
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from utils.proxy_manager import ProxyManager
from utils.nlp_tools import is_neuroscience_related

logger = logging.getLogger(__name__)


class ArxivCollector:
    """
    用于从arXiv爬取神经科学相关论文的爬虫
    使用arXiv官方API进行查询
    """

    def __init__(self, config):
        self.config = config
        self.base_url = "http://export.arxiv.org/api/query"
        self.proxy_manager = ProxyManager()
        self.neuroscience_categories = [
            "q-bio.NC",  # 神经科学和认知
            "q-bio.QM",  # 量化方法
            "stat.ML",  # 机器学习(可能包含神经网络研究)
            "cs.LG",  # 机器学习(计算机科学)
            "cs.AI",  # 人工智能
            "cs.CV",  # 计算机视觉
            "cs.NE",  # 神经进化计算
        ]
        self.neuroscience_keywords = [
            "neuroscience", "neural", "brain", "neuron", "cortex",
            "cognition", "cognitive", "eeg", "fmri", "meg", "spike",
            "action potential", "neuroimaging", "connectome"
        ]

    def _build_query(self, days_back=7):
        """构建arXiv API查询"""
        categories = " OR ".join([f"cat:{cat}" for cat in self.neuroscience_categories])
        keyword_query = " OR ".join([f"all:{kw}" for kw in self.neuroscience_keywords])

        # 添加日期范围
        date_start = (datetime.now() - timedelta(days=days_back)).strftime('%Y%m%d')
        date_end = datetime.now().strftime('%Y%m%d')
        date_query = f"submittedDate:[{date_start}2000 TO {date_end}2359]"

        # 完整查询
        query = f"({categories}) AND ({keyword_query}) AND {date_query}"
        return query

    def collect_papers(self, max_results=100):
        """收集最近的神经科学相关论文"""
        query = self._build_query()
        params = {
            'search_query': query,
            'start': 0,
            'max_results': max_results,
            'sortBy': 'submittedDate',
            'sortOrder': 'descending'
        }

        try:
            # 使用代理轮换避免被封
            proxy = self.proxy_manager.get_proxy()
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }

            response = requests.get(
                self.base_url,
                params=params,
                headers=headers,
                proxies=proxy,
                timeout=30
            )

            if response.status_code != 200:
                logger.error(f"arXiv API请求失败: {response.status_code}")
                return []

            feed = feedparser.parse(response.content)
            papers = []

            for entry in feed.entries:
                # 再次检查是否为神经科学相关(可能有边缘相关的论文也被包含在查询结果中)
                if is_neuroscience_related(entry.title + " " + entry.summary):
                    paper = {
                        'source': 'arxiv',
                        'id': entry.id.split('/')[-1],
                        'title': entry.title,
                        'authors': [author.name for author in entry.authors],
                        'abstract': entry.summary,
                        'url': entry.link,
                        'pdf_url': next((l.href for l in entry.links if l.title == 'pdf'), None),
                        'published_date': datetime.strptime(entry.published, "%Y-%m-%dT%H:%M:%SZ"),
                        'categories': [tag['term'] for tag in entry.tags],
                        'raw_data': entry
                    }
                    papers.append(paper)

                    # 模拟真实用户行为，随机等待一段时间
                    time.sleep(random.uniform(1, 3))

            logger.info(f"从arXiv收集了 {len(papers)} 篇论文")
            return papers

        except Exception as e:
            logger.error(f"从arXiv收集论文时出错: {e}")
            return []

    def extract_datasets(self, paper):
        """从论文中提取数据集信息"""
        datasets = []

        try:
            # 获取论文全文(PDF)进行分析
            if paper.get('pdf_url'):
                # 下载PDF并解析内容(这里简化处理)
                # 实际实现需要使用PDF解析库如PyPDF2, pdfminer等

                # 模拟: 从摘要和标题中寻找数据集相关关键词
                text = paper['title'] + ' ' + paper['abstract']

                # 数据集关键词匹配
                dataset_keywords = [
                    'dataset', 'data set', 'corpus', 'database',
                    'repository', 'benchmark', 'collection'
                ]

                for keyword in dataset_keywords:
                    if keyword.lower() in text.lower():
                        # 提取提及的数据集
                        # 这里仅作示例，实际应用中可能需要更复杂的NLP技术
                        match = re.search(f"([A-Z0-9-]+)(?:\\s+{keyword})", text, re.IGNORECASE)
                        if match:
                            dataset_name = match.group(1)
                            datasets.append({
                                'name': dataset_name,
                                'paper_id': paper['id'],
                                'paper_title': paper['title'],
                                'source': 'arxiv',
                                'description': f"Mentioned in {paper['title']}",
                                'url': None  # 需要进一步分析提取
                            })

                # 查找DOI链接中的数据集仓库
                if 'doi.org' in text:
                    # 提取DOI并检查是否链接到数据集
                    doi_matches = re.findall(r'doi\.org/([^\s]+)', text)
                    for doi in doi_matches:
                        datasets.append({
                            'name': f"DOI Dataset {doi}",
                            'paper_id': paper['id'],
                            'paper_title': paper['title'],
                            'source': 'arxiv',
                            'description': f"DOI referenced in {paper['title']}",
                            'url': f"https://doi.org/{doi}"
                        })

            return datasets

        except Exception as e:
            logger.error(f"提取数据集时出错: {e}, paper_id: {paper.get('id')}")
            return []