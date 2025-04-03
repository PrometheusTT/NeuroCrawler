#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import time
import random
import re
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs, urlencode
import json

from utils.proxy_manager import ProxyManager
from utils.browser_emulator import BrowserEmulator
from utils.nlp_tools import is_neuroscience_related, extract_keywords, extract_dataset_links

logger = logging.getLogger(__name__)


class CellCollector:
    """
    用于从Cell及其子刊爬取神经科学相关论文和数据集的爬虫
    支持时间范围设置，定向提取神经科学特定数据类型
    """

    def __init__(self, config):
        self.config = config
        self.proxy_manager = ProxyManager()
        self.browser = BrowserEmulator()

        # 期刊信息
        self.journals = {
            'cell': {
                'name': 'Cell',
                'base_url': 'https://www.cell.com/cell',
                'search_url': 'https://www.cell.com/action/doSearch',
                'api_url': 'https://www.cell.com/pb/api/search'
            },
            'neuron': {
                'name': 'Neuron',
                'base_url': 'https://www.cell.com/neuron',
                'search_url': 'https://www.cell.com/action/doSearch',
                'api_url': 'https://www.cell.com/pb/api/search'
            },
            'cell-reports': {
                'name': 'Cell Reports',
                'base_url': 'https://www.cell.com/cell-reports',
                'search_url': 'https://www.cell.com/action/doSearch',
                'api_url': 'https://www.cell.com/pb/api/search'
            },
            'current-biology': {
                'name': 'Current Biology',
                'base_url': 'https://www.cell.com/current-biology',
                'search_url': 'https://www.cell.com/action/doSearch',
                'api_url': 'https://www.cell.com/pb/api/search'
            },
            'cell-systems': {
                'name': 'Cell Systems',
                'base_url': 'https://www.cell.com/cell-systems',
                'search_url': 'https://www.cell.com/action/doSearch',
                'api_url': 'https://www.cell.com/pb/api/search'
            },
            'patterns': {
                'name': 'Patterns',
                'base_url': 'https://www.cell.com/patterns',
                'search_url': 'https://www.cell.com/action/doSearch',
                'api_url': 'https://www.cell.com/pb/api/search'
            },
            'cell-metabolism': {
                'name': 'Cell Metabolism',
                'base_url': 'https://www.cell.com/cell-metabolism',
                'search_url': 'https://www.cell.com/action/doSearch',
                'api_url': 'https://www.cell.com/pb/api/search'
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
        """格式化日期为Cell搜索所需格式"""
        return date.strftime("%Y-%m-%d")

    def _search_articles_api(self, journal_id, start_date, end_date, page=0, page_size=20):
        """通过Cell API搜索文章"""
        journal_info = self.journals.get(journal_id)
        if not journal_info:
            logger.error(f"未知的期刊ID: {journal_id}")
            return []

        # 构建查询参数
        params = {
            'journalcode': journal_id,
            'startPage': page,
            'resultsPerPage': page_size,
            'sortType': 'Date',
            'sortOrder': 'Descending',
            'filterKeyword': 'All',
            'startDate': self._format_date(start_date),
            'endDate': self._format_date(end_date),
            'format': 'json'
        }

        # 神经科学关键词查询
        neuro_keywords = " OR ".join([f'"{keyword}"' for keyword in self.neuroscience_keywords])
        data_keywords = " OR ".join([f'"{keyword}"' for keyword in self.target_data_keywords])
        params['searchText'] = f'({neuro_keywords}) AND ({data_keywords})'

        try:
            # 使用代理和UA轮换
            proxy = self.proxy_manager.get_proxy()
            headers = {
                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'application/json',
                'Referer': journal_info['base_url'],
                'X-Requested-With': 'XMLHttpRequest',
                'Content-Type': 'application/json'
            }

            # Cell使用POST请求进行API搜索
            response = requests.post(
                journal_info['api_url'],
                json=params,
                headers=headers,
                proxies=proxy,
                timeout=30
            )

            if response.status_code != 200:
                logger.error(f"搜索请求失败: {response.status_code}, {response.text}")
                return []

            try:
                search_results = response.json()
            except json.JSONDecodeError:
                logger.error(f"解析JSON响应失败: {response.text[:200]}...")
                return []

            articles = []

            # Cell API返回结构
            if 'results' in search_results:
                for item in search_results['results']:
                    try:
                        # 提取文章信息
                        title = item.get('title', '').strip()

                        # 构建文章URL
                        article_url = None
                        if 'link' in item:
                            article_url = item['link']
                        elif 'doi' in item:
                            article_url = f"https://www.cell.com/article/{item['doi']}"
                        else:
                            # 尝试从Pii构建URL
                            if 'pii' in item:
                                article_url = f"{journal_info['base_url']}/fulltext/{item['pii']}"

                        if not article_url:
                            continue

                        # 解析发布日期
                        pub_date = None
                        if 'date' in item:
                            try:
                                pub_date = datetime.strptime(item['date'], "%Y-%m-%d")
                            except ValueError:
                                pub_date = None

                        # 提取作者
                        authors = []
                        if 'authors' in item and isinstance(item['authors'], list):
                            authors = [author.strip() for author in item['authors']]
                        elif 'authors' in item and isinstance(item['authors'], str):
                            authors = [author.strip() for author in item['authors'].split(',')]

                        # 提取DOI
                        doi = item.get('doi')

                        # 提取摘要
                        abstract = item.get('abstract', '').strip()

                        article = {
                            'title': title,
                            'url': article_url,
                            'doi': doi,
                            'published_date': pub_date,
                            'authors': authors,
                            'journal': journal_info['name'],
                            'source': 'cell',
                            'abstract': abstract
                        }

                        articles.append(article)

                    except Exception as e:
                        logger.error(f"解析文章数据时出错: {e}")

            logger.info(f"从 {journal_info['name']} 搜索到 {len(articles)} 篇文章")
            return articles

        except Exception as e:
            logger.error(f"搜索Cell文章时出错: {e}, journal: {journal_id}")
            return []

    def _search_articles_fallback(self, journal_id, start_date, end_date, page=0, page_size=20):
        """备用方法：通过模拟浏览器搜索文章"""
        journal_info = self.journals.get(journal_id)
        if not journal_info:
            logger.error(f"未知的期刊ID: {journal_id}")
            return []

        # 构建查询URL
        params = {
            'journalCode': journal_id,
            'startPage': page,
            'pageSize': page_size,
            'sortBy': 'date',
            'sortOrder': 'descending',
            'filterSpecial': '',
            'field': 'AllField',
            'articleTypes': '',
            'startDate': self._format_date(start_date),
            'endDate': self._format_date(end_date),
        }

        # 神经科学关键词查询
        neuro_keywords = " OR ".join([f'"{keyword}"' for keyword in self.neuroscience_keywords])
        data_keywords = " OR ".join([f'"{keyword}"' for keyword in self.target_data_keywords])
        params['searchTerm'] = f'({neuro_keywords}) AND ({data_keywords})'

        search_url = f"{journal_info['search_url']}?{urlencode(params)}"

        try:
            # 使用浏览器模拟器获取页面
            html_content = self.browser.get_page(search_url, use_selenium=True)

            if not html_content:
                logger.error(f"获取搜索页面失败: {search_url}")
                return []

            soup = BeautifulSoup(html_content, 'html.parser')

            articles = []

            # Cell网站文章列表
            article_elements = soup.select('.search-result-item, .article-item')

            for article_el in article_elements:
                try:
                    # 提取标题和链接
                    title_el = article_el.select_one('h3 a, .article-title a')
                    if not title_el:
                        continue

                    title = title_el.text.strip()
                    article_url = urljoin(journal_info['base_url'], title_el['href'])

                    # 提取发布日期
                    date_el = article_el.select_one('.article-header__date, .article-info__date')
                    pub_date = None
                    if date_el:
                        date_text = date_el.text.strip()
                        try:
                            # 尝试多种日期格式
                            for fmt in ['%d %B %Y', '%B %d, %Y', '%Y-%m-%d']:
                                try:
                                    pub_date = datetime.strptime(date_text, fmt)
                                    break
                                except ValueError:
                                    continue
                        except Exception:
                            pass

                    # 提取作者
                    authors_el = article_el.select('.article-header__authors span, .article-info__authors')
                    authors = []
                    if authors_el:
                        authors_text = authors_el[0].text.strip()
                        authors = [author.strip() for author in authors_text.split(',') if author.strip()]

                    # 提取DOI
                    doi_el = article_el.select_one('.article-header__doi, .article-info__doi')
                    doi = None
                    if doi_el:
                        doi_match = re.search(r'doi\.org/([^\s]+)', doi_el.text)
                        if doi_match:
                            doi = doi_match.group(1)

                    # 提取摘要
                    abstract_el = article_el.select_one('.article-body__abstract p, .search-result-item__text')
                    abstract = abstract_el.text.strip() if abstract_el else None

                    article = {
                        'title': title,
                        'url': article_url,
                        'doi': doi,
                        'published_date': pub_date,
                        'authors': authors,
                        'journal': journal_info['name'],
                        'source': 'cell',
                        'abstract': abstract
                    }

                    articles.append(article)

                except Exception as e:
                    logger.error(f"解析文章元素时出错: {e}")

            logger.info(f"从 {journal_info['name']} 搜索到 {len(articles)} 篇文章 (备用方法)")
            return articles

        except Exception as e:
            logger.error(f"备用搜索Cell文章时出错: {e}, journal: {journal_id}")
            return []

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
            doi_el = soup.select_one('meta[name="citation_doi"], meta[name="DOI"], .doi')
            if doi_el:
                if 'content' in doi_el.attrs:
                    doi = doi_el['content']
                else:
                    # 尝试从文本中提取
                    doi_match = re.search(r'doi\.org/([^\s]+)', doi_el.text)
                    if doi_match:
                        doi = doi_match.group(1)

            # 提取摘要
            abstract = None
            abstract_el = soup.select_one('#abstracts, .article__abstract, section.section--abstract')
            if abstract_el:
                abstract = abstract_el.text.strip()

            # 提取PDF链接
            pdf_url = None
            pdf_link = soup.select_one('a.article-tools__item--pdf, a.article-tools__pdf, a[data-article-tool="pdf"]')
            if pdf_link and 'href' in pdf_link.attrs:
                pdf_url = urljoin(article_url, pdf_link['href'])

            # 提取补充材料链接
            supplementary_url = None
            supp_link = soup.select_one('a.article-tools__item--supplemental, a.article-tools__supplemental')
            if supp_link and 'href' in supp_link.attrs:
                supplementary_url = urljoin(article_url, supp_link['href'])

            # 提取STAR Methods链接 (Cell的特殊部分，通常包含方法和数据可用性)
            star_methods_url = None
            star_link = soup.select_one('a.article-tools__item--methods, a.article-tools__methods')
            if star_link and 'href' in star_link.attrs:
                star_methods_url = urljoin(article_url, star_link['href'])

            details = {
                'abstract': abstract,
                'doi': doi,
                'pdf_url': pdf_url,
                'supplementary_url': supplementary_url,
                'star_methods_url': star_methods_url
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

            # Cell的数据可用性部分通常在STAR Methods中
            data_availability_section = None

            # 首先检查是否在当前页面上
            data_sections = soup.select('section.section--data-availability, div.section[data-section-id="data-availability"]')

            if data_sections:
                data_availability_section = data_sections[0]
            else:
                # Cell经常将STAR Methods放在单独的页面上，需要额外请求
                if article_details.get('star_methods_url'):
                    try:
                        methods_content = self.browser.get_page(article_details['star_methods_url'], use_selenium=True)
                        if methods_content:
                            methods_soup = BeautifulSoup(methods_content, 'html.parser')

                            # 查找数据可用性部分
                            data_sections = methods_soup.select('section.section--data-availability, div.section[data-section-id="data-availability"]')
                            if data_sections:
                                data_availability_section = data_sections[0]
                    except Exception as e:
                        logger.error(f"获取STAR Methods页面时出错: {e}")

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
                            'source': 'cell',
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
                                'source': 'cell',
                                'data_types': article_details.get('target_data_types', []),
                                'doi': match
                            }
                            datasets.append(dataset)

                    # 查找Accession number模式 (Cell经常使用)
                    accession_patterns = [
                        r'accession (?:code|number)[:\s]+([^\s\.,;]+)',
                        r'accession[:\s]+([^\s\.,;]+)',
                        r'accession numbers are ([^\s\.,;]+(?:,\s*[^\s\.,;]+)*)',
                        r'([A-Z]{1,3}\d{5,})'  # 通用的Accession number模式
                    ]
                    for pattern in accession_patterns:
                        matches = re.findall(pattern, data_text, re.IGNORECASE)
                        for match in matches:
                            if isinstance(match, tuple):
                                match = match[0]

                            # 处理多个accession numbers的情况
                            if ',' in match:
                                accessions = [acc.strip() for acc in match.split(',')]
                                for acc in accessions:
                                    if acc:
                                        dataset = {
                                            'name': f"Dataset Accession: {acc}",
                                            'url': None,  # 无法直接生成URL
                                            'repository': 'Accession',
                                            'source': 'cell',
                                            'accession': acc,
                                            'data_types': article_details.get('target_data_types', []),
                                            'doi': article_details.get('doi')
                                        }
                                        datasets.append(dataset)
                            else:
                                dataset = {
                                    'name': f"Dataset Accession: {match}",
                                    'url': None,  # 无法直接生成URL
                                    'repository': 'Accession',
                                    'source': 'cell',
                                    'accession': match,
                                    'data_types': article_details.get('target_data_types', []),
                                    'doi': article_details.get('doi')
                                }
                                datasets.append(dataset)

            # Cell经常将数据引用放在Key Resources Table中
            resource_tables = soup.select('div.table-key-resources, table.e-component-table, table.supplementary-material')

            for table in resource_tables:
                rows = table.select('tr')
                for row in rows:
                    cells = row.select('td')
                    if len(cells) >= 2:
                        # 检查是否为数据相关行
                        row_text = row.text.lower()
                        if any(term in row_text for term in ['data', 'dataset', 'database', 'accession', 'repository']):
                            # 提取链接
                            links = row.select('a')
                            for link in links:
                                link_url = link.get('href', '')
                                link_text = link.text.strip()

                                if link_url and (link_url.startswith('http') or link_url.startswith('/')):
                                    dataset = {
                                        'name': link_text if link_text else f"Dataset from Resource Table",
                                        'url': link_url if link_url.startswith('http') else urljoin(article_url, link_url),
                                        'repository': 'resource_table',
                                        'source': 'cell',
                                        'data_types': article_details.get('target_data_types', []),
                                        'doi': article_details.get('doi')
                                    }
                                    datasets.append(dataset)

            # 检查补充材料中的数据文件
            if article_details.get('supplementary_url'):
                try:
                    supp_content = self.browser.get_page(article_details['supplementary_url'], use_selenium=True)
                    if supp_content:
                        supp_soup = BeautifulSoup(supp_content, 'html.parser')

                        # 查找补充材料文件
                        supp_files = supp_soup.select('a.download-link, a.download, a[data-download]')

                        # 数据文件扩展名
                        data_extensions = ['.csv', '.tsv', '.xlsx', '.xls', '.zip', '.gz', '.tar',
                                         '.nii', '.nii.gz', '.mat', '.h5', '.hdf5', '.txt', '.fasta']

                        for supp_file in supp_files:
                            file_url = supp_file.get('href', '')
                            file_text = supp_file.text.strip()

                            if file_url and any(file_url.lower().endswith(ext) for ext in data_extensions):
                                dataset = {
                                    'name': file_text if file_text else f"Supplementary Data {file_url.split('/')[-1]}",
                                    'url': file_url if file_url.startswith('http') else urljoin(article_details['supplementary_url'], file_url),
                                    'repository': 'supplementary_materials',
                                    'source': 'cell',
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
                    'source': 'cell',
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
        logger.info(f"正在从Cell收集{start_date}到{end_date}之间的神经科学论文")

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

            # 尝试使用API搜索，如果失败则回退到备用方法
            try:
                articles = self._search_articles_api(journal_id, start_date, end_date)
            except Exception as e:
                logger.warning(f"API搜索失败，使用备用方法: {e}")
                articles = self._search_articles_fallback(journal_id, start_date, end_date)

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
                            logger.info(f"发现含有目标数据的论文: {article['title']}, 数据类型: {article_details.get('target_data_types', [])}")

                    # 随机等待，避免频繁请求
                    time.sleep(random.uniform(1, 3))

                except Exception as e:
                    logger.error(f"处理文章详情时出错: {e}, url: {article['url']}")

            # 每处理完一个期刊，等待一段时间
            time.sleep(random.uniform(5, 10))

        logger.info(f"从Cell收集到{len(all_papers)}篇符合条件的论文")
        return all_papers

    def extract_datasets(self, paper):
        """从论文中提取数据集"""
        if 'datasets' in paper:
            return paper['datasets']
        else:
            # 如果没有预先提取的数据集，尝试从论文URL中提取
            return self._extract_dataset_info(paper, paper['url'])