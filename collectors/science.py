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


class ScienceCollector:
    """
    用于从Science及其子刊爬取神经科学相关论文和数据集的爬虫
    支持时间范围设置，定向提取神经科学特定数据类型
    """

    def __init__(self, config):
        self.config = config
        self.proxy_manager = ProxyManager()
        self.browser = BrowserEmulator()

        # 期刊信息
        self.journals = {
            'science': {
                'name': 'Science',
                'base_url': 'https://www.science.org/journal/science',
                'search_url': 'https://www.science.org/action/doSearch',
            },
            'science-advances': {
                'name': 'Science Advances',
                'base_url': 'https://www.science.org/journal/sciadv',
                'search_url': 'https://www.science.org/action/doSearch',
            },
            'science-translational-medicine': {
                'name': 'Science Translational Medicine',
                'base_url': 'https://www.science.org/journal/stm',
                'search_url': 'https://www.science.org/action/doSearch',
            },
            'science-signaling': {
                'name': 'Science Signaling',
                'base_url': 'https://www.science.org/journal/signaling',
                'search_url': 'https://www.science.org/action/doSearch',
            },
            'science-immunology': {
                'name': 'Science Immunology',
                'base_url': 'https://www.science.org/journal/sciimmunol',
                'search_url': 'https://www.science.org/action/doSearch',
            },
            'science-robotics': {
                'name': 'Science Robotics',
                'base_url': 'https://www.science.org/journal/scirobotics',
                'search_url': 'https://www.science.org/action/doSearch',
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
        """格式化日期为Science搜索所需格式"""
        return date.strftime("%Y-%m-%d")

    def _search_articles(self, journal_id, start_date, end_date, page=0, page_size=20):
        """使用Science搜索API搜索文章"""
        journal_info = self.journals.get(journal_id)
        if not journal_info:
            logger.error(f"未知的期刊ID: {journal_id}")
            return []

        # 构建查询参数
        params = {
            'publication': journal_id,
            'startPage': page,
            'pageSize': page_size,
            'sortOption.sort': 'Date-Newest_First',
            'date': 'range',
            'dateFilterValue1': self._format_date(start_date),
            'dateFilterValue2': self._format_date(end_date),
            'searchType': 'advanced',
            'searchScope': 'AllContent',
            'format': 'json'
        }

        # 添加神经科学关键词进行过滤（使用OR连接所有关键词）
        neuro_keywords_query = " OR ".join([f'"{keyword}"' for keyword in self.neuroscience_keywords])
        data_keywords_query = " OR ".join([f'"{keyword}"' for keyword in self.target_data_keywords])

        # Science的高级搜索语法
        params['queryStr'] = f'(({neuro_keywords_query}) AND ({data_keywords_query}))'

        try:
            # 使用代理和UA轮换
            proxy = self.proxy_manager.get_proxy()
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'application/json',
                'Referer': journal_info['base_url']
            }

            search_url = journal_info['search_url']

            response = requests.get(
                search_url,
                params=params,
                headers=headers,
                proxies=proxy,
                timeout=30
            )

            if response.status_code != 200:
                logger.error(f"搜索请求失败: {response.status_code}, {response.text}")
                return []

            try:
                # 解析JSON响应
                search_results = response.json()
            except json.JSONDecodeError:
                # 如果不是JSON，尝试解析HTML
                soup = BeautifulSoup(response.text, 'html.parser')
                articles = self._parse_search_results_html(soup, journal_info)
                return articles

            # 解析JSON结果
            articles = []

            if 'items' in search_results:
                for item in search_results['items']:
                    try:
                        article = {
                            'title': item.get('title', '').strip(),
                            'url': urljoin(search_url, item.get('link', '')),
                            'doi': item.get('doi'),
                            'published_date': self._parse_date(item.get('publicationDate')),
                            'authors': [author.strip() for author in item.get('authors', '').split(',') if
                                        author.strip()],
                            'journal': journal_info['name'],
                            'source': 'science',
                            'abstract': item.get('abstract')
                        }
                        articles.append(article)
                    except Exception as e:
                        logger.error(f"解析文章数据时出错: {e}")

            logger.info(f"从 {journal_info['name']} 搜索到 {len(articles)} 篇文章")
            return articles

        except Exception as e:
            logger.error(f"搜索Science文章时出错: {e}, journal: {journal_id}")
            return []

    def _parse_search_results_html(self, soup, journal_info):
        """解析HTML格式的搜索结果"""
        articles = []

        # Science网站的文章列表选择器
        article_elements = soup.select('.card-body, .issue-item, .searchResultItem')

        for article_el in article_elements:
            try:
                # 提取标题和URL
                title_el = article_el.select_one('h2 a, .issue-item__title a, .meta__title a')
                if not title_el:
                    continue

                title = title_el.text.strip()
                article_url = urljoin(journal_info['base_url'], title_el['href'])

                # 提取DOI
                doi_el = article_el.select_one('a.issue-item__doi, .meta__doi')
                doi = None
                if doi_el:
                    doi_match = re.search(r'doi\.org/([^/\s]+)', doi_el.text)
                    if doi_match:
                        doi = doi_match.group(1)

                # 提取发布日期
                date_el = article_el.select_one('.card-meta__date, .issue-item__date, .meta__date')
                pub_date = None
                if date_el:
                    date_text = date_el.text.strip()
                    try:
                        # 尝试多种日期格式
                        for fmt in ['%d %b %Y', '%B %d, %Y', '%Y-%m-%d']:
                            try:
                                pub_date = datetime.strptime(date_text, fmt)
                                break
                            except ValueError:
                                continue
                    except Exception:
                        pass

                # 提取作者
                authors_el = article_el.select('.card-meta__authors, .issue-item__authors, .meta__authors, .loa')
                authors = []
                if authors_el:
                    authors_text = authors_el[0].text.strip()
                    authors = [author.strip() for author in authors_text.split(',') if author.strip()]

                # 提取摘要
                abstract_el = article_el.select_one('.issue-item__abstract, .meta__abstract')
                abstract = abstract_el.text.strip() if abstract_el else None

                article = {
                    'title': title,
                    'url': article_url,
                    'doi': doi,
                    'published_date': pub_date,
                    'authors': authors,
                    'journal': journal_info['name'],
                    'source': 'science',
                    'abstract': abstract
                }

                articles.append(article)

            except Exception as e:
                logger.error(f"解析HTML文章元素时出错: {e}")

        return articles

    def _parse_date(self, date_str):
        """解析日期字符串为datetime对象"""
        if not date_str:
            return None

        try:
            # 尝试多种日期格式
            for fmt in ['%Y-%m-%d', '%d %b %Y', '%B %d, %Y']:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue

            # 如果是时间戳
            if date_str.isdigit():
                return datetime.fromtimestamp(int(date_str))

            return None
        except Exception:
            return None

    def _get_article_details(self, article_url):
        """获取文章详细信息"""
        try:
            # 使用浏览器模拟器获取页面
            html_content = self.browser.get_page(article_url, use_selenium=True)

            if not html_content:
                logger.error(f"获取文章详情页面失败: {article_url}")
                return {}

            soup = BeautifulSoup(html_content, 'html.parser')

            # 提取DOI (如果尚未提取)
            doi = None
            doi_el = soup.select_one('meta[name="citation_doi"], meta[name="DOI"]')
            if doi_el:
                doi = doi_el['content']
            else:
                # 尝试从页面文本中提取
                doi_spans = soup.select('.citation__doi, .article__doi span')
                for span in doi_spans:
                    doi_match = re.search(r'doi\.org/([^\s]+)', span.text)
                    if doi_match:
                        doi = doi_match.group(1)
                        break

            # 提取摘要
            abstract = None
            abstract_el = soup.select_one('#abstract, .section__abstract, .article__abstract')
            if abstract_el:
                abstract = abstract_el.text.strip()

            # 提取PDF链接
            pdf_url = None
            pdf_link = soup.select_one('a[data-track-action="download pdf"], a.article__toollink--pdf')
            if pdf_link and 'href' in pdf_link.attrs:
                pdf_url = urljoin(article_url, pdf_link['href'])

            # 提取补充材料链接
            supplementary_url = None
            supp_link = soup.select_one(
                'a[data-track-action="supplementary materials"], a.article__toollink--materials')
            if supp_link and 'href' in supp_link.attrs:
                supplementary_url = urljoin(article_url, supp_link['href'])

            details = {
                'abstract': abstract,
                'doi': doi,
                'pdf_url': pdf_url,
                'supplementary_url': supplementary_url
            }

            # 判断是否与目标数据类型相关
            combined_text = ' '.join(filter(None, [abstract, soup.get_text()]))
            if combined_text:
                for keyword in self.target_data_keywords:
                    if keyword.lower() in combined_text.lower():
                        details['contains_target_data'] = True
                        details['target_data_types'] = self._identify_data_types(combined_text)
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
        """从文章详情中提取数据集信息"""
        datasets = []

        try:
            # 使用浏览器模拟器获取页面
            html_content = self.browser.get_page(article_url, use_selenium=True)

            if not html_content:
                return datasets

            soup = BeautifulSoup(html_content, 'html.parser')

            # 查找DATA AVAILABILITY部分
            data_availability_section = None

            # Science的数据可用性部分通常在MATERIALS AND METHODS中
            method_sections = soup.select('div.section:has(h2:contains("Materials and Methods")), '
                                          'div.section:has(h2:contains("Methods"))')

            if method_sections:
                # 在方法部分中查找数据可用性相关段落
                data_keywords = ['data availability', 'availability of data', 'code availability', 'data deposition']
                for section in method_sections:
                    paragraphs = section.select('p')
                    for paragraph in paragraphs:
                        text = paragraph.text.lower()
                        if any(keyword in text for keyword in data_keywords):
                            data_availability_section = paragraph
                            break
                    if data_availability_section:
                        break

            # 也可能有单独的数据可用性部分
            if not data_availability_section:
                data_sections = soup.select('div.section:has(h2:contains("Data Availability")), '
                                            'div.section:has(h2:contains("Data and Code Availability"))')
                if data_sections:
                    data_availability_section = data_sections[0]

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
                            'source': 'science',
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
                                'source': 'science',
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
                                'source': 'science',
                                'accession': match,
                                'data_types': article_details.get('target_data_types', []),
                                'doi': article_details.get('doi')
                            }
                            datasets.append(dataset)

            # 检查补充材料中是否有数据集
            if article_details.get('supplementary_url'):
                try:
                    # 获取补充材料页面
                    supp_content = self.browser.get_page(article_details['supplementary_url'], use_selenium=True)
                    if supp_content:
                        supp_soup = BeautifulSoup(supp_content, 'html.parser')
                        supp_links = supp_soup.select('a')

                        # 数据文件扩展名
                        data_extensions = ['.csv', '.tsv', '.xlsx', '.xls', '.zip', '.gz', '.tar',
                                           '.nii', '.nii.gz', '.mat', '.h5', '.hdf5', '.txt', '.fasta']

                        for link in supp_links:
                            link_url = link.get('href', '')
                            link_text = link.text.strip()

                            # 检查是否是数据文件
                            if any(link_url.lower().endswith(ext) for ext in data_extensions):
                                dataset = {
                                    'name': link_text if link_text else f"Dataset {link_url.split('/')[-1]}",
                                    'url': link_url if link_url.startswith('http') else urljoin(
                                        article_details['supplementary_url'], link_url),
                                    'repository': 'supplementary_materials',
                                    'source': 'science',
                                    'data_types': article_details.get('target_data_types', []),
                                    'doi': article_details.get('doi')
                                }
                                datasets.append(dataset)
                except Exception as e:
                    logger.error(f"处理补充材料时出错: {e}")

            # 如果仍然没有找到数据集，但存在补充材料链接
            if not datasets and article_details.get('supplementary_url'):
                dataset = {
                    'name': "Supplementary Materials",
                    'url': article_details['supplementary_url'],
                    'repository': 'journal_supplementary',
                    'source': 'science',
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
        logger.info(f"正在从Science收集{start_date}到{end_date}之间的神经科学论文")

        all_papers = []

        # 遍历配置的期刊
        for journal_id in self.journals:
            # 如果期刊在配置中被禁用，则跳过
            # 安全地检查配置
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

        logger.info(f"从Science收集到{len(all_papers)}篇符合条件的论文")
        return all_papers

    def extract_datasets(self, paper):
        """从论文中提取数据集"""
        if 'datasets' in paper:
            return paper['datasets']
        else:
            # 如果没有预先提取的数据集，尝试从论文URL中提取
            return self._extract_dataset_info(paper, paper['url'])