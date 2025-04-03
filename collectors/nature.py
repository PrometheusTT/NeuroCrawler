#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import time
import random
import re
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json

from utils.proxy_manager import ProxyManager
from utils.browser_emulator import BrowserEmulator
from utils.nlp_tools import is_neuroscience_related, extract_keywords, extract_dataset_links

logger = logging.getLogger(__name__)


class NatureCollector:
    """
    用于从Nature及其子刊爬取神经科学相关论文和数据集的爬虫
    支持时间范围设置，定向提取神经科学特定数据类型
    """

    def __init__(self, config):
        self.config = config
        self.proxy_manager = ProxyManager()
        self.browser = BrowserEmulator()

        # 期刊信息
        self.journals = {
            'nature': {
                'name': 'Nature',
                'base_url': 'https://www.nature.com/nature',
                'advanced_search_url': 'https://www.nature.com/search',
                'articles_api': 'https://www.nature.com/nature/articles',
            },
            'nature-neuroscience': {
                'name': 'Nature Neuroscience',
                'base_url': 'https://www.nature.com/neuro',
                'advanced_search_url': 'https://www.nature.com/search',
                'articles_api': 'https://www.nature.com/neuro/articles',
            },
            'nature-methods': {
                'name': 'Nature Methods',
                'base_url': 'https://www.nature.com/nmeth',
                'advanced_search_url': 'https://www.nature.com/search',
                'articles_api': 'https://www.nature.com/nmeth/articles',
            },
            'nature-communications': {
                'name': 'Nature Communications',
                'base_url': 'https://www.nature.com/ncomms',
                'advanced_search_url': 'https://www.nature.com/search',
                'articles_api': 'https://www.nature.com/ncomms/articles',
            },
            'scientific-reports': {
                'name': 'Scientific Reports',
                'base_url': 'https://www.nature.com/srep',
                'advanced_search_url': 'https://www.nature.com/search',
                'articles_api': 'https://www.nature.com/srep/articles',
            },
            'nature-machine-intelligence': {
                'name': 'Nature Machine Intelligence',
                'base_url': 'https://www.nature.com/natmachintell',
                'advanced_search_url': 'https://www.nature.com/search',
                'articles_api': 'https://www.nature.com/natmachintell/articles',
            },
            'nature-biotechnology': {
                'name': 'Nature Biotechnology',
                'base_url': 'https://www.nature.com/nbt',
                'advanced_search_url': 'https://www.nature.com/search',
                'articles_api': 'https://www.nature.com/nbt/articles',
            }
        }

        # 神经科学关键词
        self.neuroscience_keywords = [
            "neuroscience", "neural", "brain", "neuron", "cortex",
            "cognition", "cognitive", "neuroimaging", "connectome",
            "neuroinformatics", "computational neuroscience"
        ]

        # 目标数据类型关键词
        self.target_data_keywords = [
            # 神经元图像数据
            "neuron imaging", "neuron morphology", "calcium imaging",
            "neuron reconstruction", "neuronal activity", "neuronal image",

            # 重建数据
            "reconstruction", "3D reconstruction", "connectome", "neuronal circuit",
            "circuit reconstruction", "neural circuit", "neural reconstruction",

            # 空间转录组数据
            "spatial transcriptomics", "single-cell RNA-seq", "scRNA-seq",
            "spatial gene expression", "spatially resolved transcriptomics",
            "spatial mapping RNA", "spatial omics",

            # MRI数据
            "MRI", "fMRI", "magnetic resonance imaging", "functional MRI",
            "diffusion MRI", "diffusion tensor", "structural MRI",
            "brain imaging", "tractography", "connectivity",

            # 电生理数据
            "electrophysiology", "patch clamp", "whole-cell recording",
            "spike sorting", "EEG", "MEG", "LFP", "local field potential",
            "action potential", "neural recording", "electrode array",
            "multi-electrode array", "ephys"
        ]

        # 判断是否是首次运行
        self.is_first_run = True

    def _get_time_range(self):
        """获取时间范围"""
        end_date = datetime.now()

        # 第一次运行爬取过去一个月的数据，后续只爬取当天的数据
        if self.is_first_run:
            start_date = end_date - timedelta(days=30)
            self.is_first_run = False
        else:
            start_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

        return start_date, end_date

    def _format_date(self, date):
        """格式化日期为Nature API所需格式"""
        return date.strftime("%Y-%m-%d")

    def _search_articles(self, journal_id, start_date, end_date, page=1, page_size=100):
        """通过Nature高级搜索API搜索文章"""
        journal_info = self.journals.get(journal_id)
        if not journal_info:
            logger.error(f"未知的期刊ID: {journal_id}")
            return []

        # 构建查询参数
        params = {
            'journal': journal_id,
            'date_range': f'{self._format_date(start_date)} TO {self._format_date(end_date)}',
            'order': 'date_desc',
            'page': page,
            'page_size': page_size,
            'nature_research': 'yes'
        }

        # 修改搜索策略：使用OR而非AND连接数据类型关键词
        # 这将大大增加匹配的可能性
        neuro_search_terms = " OR ".join([f'"{keyword}"' for keyword in self.neuroscience_keywords])

        # 直接使用神经科学关键词，不再要求同时匹配数据类型
        params['q'] = f'({neuro_search_terms})'

        # 记录完整查询URL以便调试
        logger.info(f"搜索查询: {params['q']}")

        try:
            # 使用浏览器模拟器获取页面，处理JavaScript加载的内容
            proxy = self.proxy_manager.get_proxy()
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'application/json',
                'Referer': journal_info['base_url']
            }

            search_url = journal_info['advanced_search_url']

            # 构建查询字符串
            query_params = []
            for key, value in params.items():
                query_params.append(f"{key}={requests.utils.quote(str(value))}")
            full_url = f"{search_url}?{'&'.join(query_params)}"

            # 使用浏览器模拟器加载页面
            html_content = self.browser.get_page(full_url, use_selenium=True)

            if not html_content:
                logger.error(f"获取搜索页面失败: {full_url}")
                return []

            # 解析搜索结果
            soup = BeautifulSoup(html_content, 'html.parser')

            articles = []
            article_elements = soup.select('li.app-article-list-row')

            for article_el in article_elements:
                try:
                    # 提取文章信息
                    title_el = article_el.select_one('h3.c-card__title a')
                    if not title_el:
                        continue

                    title = title_el.text.strip()
                    article_url = urljoin(search_url, title_el['href'])

                    # 提取发布日期
                    date_el = article_el.select_one('time')
                    pub_date = None
                    if date_el and date_el.get('datetime'):
                        try:
                            pub_date = datetime.strptime(date_el['datetime'], "%Y-%m-%d")
                        except ValueError:
                            try:
                                pub_date = datetime.strptime(date_el.text.strip(), "%d %b %Y")
                            except ValueError:
                                pub_date = None

                    # 提取作者信息
                    authors_el = article_el.select('ul.c-author-list li')
                    authors = [author.text.strip() for author in authors_el]

                    # 初步收集文章信息
                    article = {
                        'title': title,
                        'url': article_url,
                        'published_date': pub_date,
                        'authors': authors,
                        'journal': journal_info['name'],
                        'source': 'nature',
                        'abstract': None,
                        'doi': None
                    }

                    articles.append(article)

                except Exception as e:
                    logger.error(f"解析文章元素时出错: {e}")

            logger.info(f"从 {journal_info['name']} 搜索到 {len(articles)} 篇文章")
            return articles

        except Exception as e:
            logger.error(f"搜索Nature文章时出错: {e}, journal: {journal_id}")
            return []

    def _get_article_details(self, article_url):
        """获取文章详细信息"""
        try:
            # 使用浏览器模拟器获取页面，处理JavaScript加载的内容
            html_content = self.browser.get_page(article_url, use_selenium=True)

            if not html_content:
                logger.error(f"获取文章详情页面失败: {article_url}")
                return {}

            soup = BeautifulSoup(html_content, 'html.parser')

            # 提取DOI
            doi_el = soup.select_one('meta[name="DOI"], meta[name="doi"], meta[property="og:doi"]')
            doi = doi_el['content'] if doi_el else None

            # 提取摘要
            abstract_el = soup.select_one(
                'div#abstract, div.c-article-section[data-title="Abstract"] p, section#abstract p')
            abstract = abstract_el.text.strip() if abstract_el else None

            # 提取PDF链接
            pdf_el = soup.select_one('a.c-pdf-download__link, a[data-track-action="download pdf"]')
            pdf_url = urljoin(article_url, pdf_el['href']) if pdf_el and 'href' in pdf_el.attrs else None

            # 提取补充材料链接
            supp_el = soup.select_one('a[data-track-action="supplementary information"]')
            supplementary_url = urljoin(article_url, supp_el['href']) if supp_el and 'href' in supp_el.attrs else None

            details = {
                'abstract': abstract,
                'doi': doi,
                'pdf_url': pdf_url,
                'supplementary_url': supplementary_url
            }

            # 判断是否与目标数据类型相关
            if abstract:
                for keyword in self.target_data_keywords:
                    if keyword.lower() in abstract.lower():
                        details['contains_target_data'] = True
                        details['target_data_types'] = self._identify_data_types(abstract)
                        break

            return details

        except Exception as e:
            logger.error(f"获取文章详情时出错: {e}, url: {article_url}")
            return {}

    def _identify_data_types(self, text):
        """识别文本中提及的数据类型"""
        data_types = set()
        text = text.lower()

        # 神经元图像数据
        neuron_imaging_keywords = [
            "neuron imaging", "neuron morphology", "calcium imaging",
            "neuronal activity", "neuronal image"
        ]
        for kw in neuron_imaging_keywords:
            if kw in text:
                data_types.add("neuron_imaging")
                break

        # 重建数据
        reconstruction_keywords = [
            "reconstruction", "3d reconstruction", "connectome",
            "neuronal circuit", "circuit reconstruction"
        ]
        for kw in reconstruction_keywords:
            if kw in text:
                data_types.add("reconstruction")
                break

        # 空间转录组数据
        spatial_transcriptomics_keywords = [
            "spatial transcriptomics", "single-cell rna-seq", "scrna-seq",
            "spatial gene expression", "spatial omics"
        ]
        for kw in spatial_transcriptomics_keywords:
            if kw in text:
                data_types.add("spatial_transcriptomics")
                break

        # MRI数据
        mri_keywords = [
            "mri", "fmri", "magnetic resonance imaging", "diffusion mri",
            "brain imaging", "tractography"
        ]
        for kw in mri_keywords:
            if kw in text:
                data_types.add("mri")
                break

        # 电生理数据
        electrophysiology_keywords = [
            "electrophysiology", "patch clamp", "spike sorting", "eeg",
            "meg", "lfp", "action potential", "ephys"
        ]
        for kw in electrophysiology_keywords:
            if kw in text:
                data_types.add("electrophysiology")
                break

        return list(data_types)

    def _extract_dataset_info(self, article_details, article_url):
        """
        从文章详情中提取数据集信息
        包括检查DATA AVAILABILITY部分和检查特定外部数据库链接
        """
        datasets = []

        try:
            # 使用浏览器模拟器获取页面，处理JavaScript加载的内容
            html_content = self.browser.get_page(article_url, use_selenium=True)

            if not html_content:
                return datasets

            soup = BeautifulSoup(html_content, 'html.parser')

            # 查找DATA AVAILABILITY部分
            data_availability_section = None

            # 方法1: 直接找Data availability或Availability部分
            data_sections = soup.select('div.c-article-section[data-title="Data availability"], '
                                        'div.c-article-section[data-title="Availability"], '
                                        'h2:contains("Data availability") + div, '
                                        'h2:contains("DATA AVAILABILITY") + div')

            if data_sections:
                data_availability_section = data_sections[0]
            else:
                # 方法2: 在文章的所有段落中查找"data availability"关键词
                paragraphs = soup.select('div.c-article-section p')
                for p in paragraphs:
                    if 'data availability' in p.text.lower():
                        # 获取该段落及其后续段落
                        data_availability_section = p.parent
                        break

            # 如果找到了数据可用性部分
            if data_availability_section:
                # 提取文本
                data_text = data_availability_section.text

                # 提取链接
                data_links = data_availability_section.select('a')

                # 常见数据仓库匹配规则
                data_repositories = {
                    'figshare': r'figshare\.com|figshare',
                    'zenodo': r'zenodo\.org|zenodo',
                    'dryad': r'datadryad\.org|dryad',
                    'osf': r'osf\.io',
                    'github': r'github\.com',
                    'gene expression omnibus': r'geo|gene expression omnibus|ncbi\.nlm\.nih\.gov\/geo',
                    'genbank': r'genbank|ncbi\.nlm\.nih\.gov\/genbank',
                    'ebi': r'ebi\.ac\.uk',
                    'neurodata': r'neurodata\.io',
                    'neurovault': r'neurovault\.org',
                    'openneuro': r'openneuro\.org',
                    'brainmaps': r'brainmaps\.org',
                    'allen brain atlas': r'brain-map\.org|allen brain',
                    'human connectome project': r'humanconnectome\.org',
                    'uk biobank': r'ukbiobank\.ac\.uk'
                }

                # 从链接中提取数据集
                for link in data_links:
                    link_url = link.get('href', '')
                    link_text = link.text.strip()

                    # 识别数据仓库
                    repository_name = None
                    for repo_name, pattern in data_repositories.items():
                        if re.search(pattern, link_url, re.IGNORECASE) or re.search(pattern, link_text, re.IGNORECASE):
                            repository_name = repo_name
                            break

                    # 如果识别出了数据仓库，添加到数据集列表
                    if repository_name:
                        dataset = {
                            'name': link_text if link_text else f"Dataset from {repository_name}",
                            'url': link_url if link_url.startswith('http') else urljoin(article_url, link_url),
                            'repository': repository_name,
                            'source': 'nature',
                            'data_types': article_details.get('target_data_types', []),
                            'doi': article_details.get('doi')
                        }
                        datasets.append(dataset)

                # 如果没有找到链接，尝试从文本中提取DOI或accession numbers
                if not datasets:
                    # 查找DOI模式
                    doi_patterns = [
                        r'doi[:\s]+([^\s]+)',
                        r'https?://doi\.org/([^\s]+)'
                    ]
                    for pattern in doi_patterns:
                        matches = re.findall(pattern, data_text, re.IGNORECASE)
                        for match in matches:
                            dataset = {
                                'name': f"Dataset DOI: {match}",
                                'url': f"https://doi.org/{match}",
                                'repository': 'DOI',
                                'source': 'nature',
                                'data_types': article_details.get('target_data_types', []),
                                'doi': match
                            }
                            datasets.append(dataset)

                    # 查找Accession number模式
                    accession_patterns = [
                        r'accession (?:code|number)[:\s]+([^\s\.,]+)',
                        r'accession[:\s]+([^\s\.,]+)',
                        r'([A-Z]{1,3}\d{5,})'  # 通用的Accession number模式
                    ]
                    for pattern in accession_patterns:
                        matches = re.findall(pattern, data_text, re.IGNORECASE)
                        for match in matches:
                            dataset = {
                                'name': f"Dataset Accession: {match}",
                                'url': None,  # 这里无法直接生成URL，因为不知道是哪个数据库
                                'repository': 'Accession',
                                'source': 'nature',
                                'accession': match,
                                'data_types': article_details.get('target_data_types', []),
                                'doi': article_details.get('doi')
                            }
                            datasets.append(dataset)

            # 如果没有找到数据集信息，检查补充材料
            if not datasets and article_details.get('supplementary_url'):
                dataset = {
                    'name': "Supplementary Materials",
                    'url': article_details['supplementary_url'],
                    'repository': 'journal_supplementary',
                    'source': 'nature',
                    'data_types': article_details.get('target_data_types', []),
                    'doi': article_details.get('doi')
                }
                datasets.append(dataset)

            return datasets

        except Exception as e:
            logger.error(f"提取数据集信息时出错: {e}, url: {article_url}")
            return datasets

    def collect_papers(self):
        """收集符合条件的论文"""
        # 获取时间范围
        start_date, end_date = self._get_time_range()
        logger.info(f"正在从Nature收集{start_date}到{end_date}之间的神经科学论文")

        all_papers = []

        # 遍历配置的期刊
        for journal_id in self.journals:
            journals_config = self.config.get('journals', {})
            if isinstance(journals_config, list):  # 如果是列表则转换为字典
                journals_config = {}

            # 如果期刊在配置中被禁用，则跳过
            if journal_id in journals_config and not journals_config.get(journal_id, {}).get('enabled', True):
                continue

            logger.info(f"正在处理期刊: {self.journals[journal_id]['name']}")

            # 搜索文章
            articles = self._search_articles(journal_id, start_date, end_date)

            # 获取每篇文章的详细信息
            for article in articles:
                try:
                    # 获取文章详情
                    article_details = self._get_article_details(article['url'])

                    # 更新文章信息
                    article.update(article_details)

                    # 检查是否包含目标数据类型
                    if article_details.get('contains_target_data', False):
                        # 提取文章中的数据集信息
                        datasets = self._extract_dataset_info(article_details, article['url'])

                        # 如果找到数据集，添加到文章中
                        if datasets:
                            article['datasets'] = datasets
                            all_papers.append(article)
                            logger.info(
                                f"发现含有目标数据的论文: {article['title']}, 数据类型: {article_details.get('target_data_types', [])}")

                    # 随机等待，避免频繁请求
                    time.sleep(random.uniform(1, 3))

                except Exception as e:
                    logger.error(f"处理文章详情时出错: {e}, url: {article['url']}")

            # 每处理完一个期刊，等待一段时间
            time.sleep(random.uniform(5, 10))

        logger.info(f"从Nature收集到{len(all_papers)}篇符合条件的论文")
        return all_papers

    def extract_datasets(self, paper):
        """从论文中提取数据集"""
        if 'datasets' in paper:
            return paper['datasets']
        else:
            # 如果没有预先提取的数据集，尝试从论文URL中提取
            return self._extract_dataset_info(paper, paper['url'])