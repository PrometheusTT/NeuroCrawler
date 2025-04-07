#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import logging
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from utils.selectors import NATURE_SELECTORS, SCIENCE_SELECTORS, CELL_SELECTORS

logger = logging.getLogger(__name__)


class DatasetExtractor:
    """专门用于从学术论文中提取数据集信息的解析器"""

    def __init__(self):
        # 常见数据仓库匹配规则
        self.data_repositories = {
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
            'uk biobank': r'ukbiobank\.ac\.uk',
            'ncbi': r'ncbi\.nlm\.nih\.gov',
            'dataverse': r'dataverse',
            'ieee dataport': r'ieee-dataport',
            'kaggle': r'kaggle\.com',
            'harvard dataverse': r'dataverse\.harvard\.edu',
            'crcns': r'crcns\.org',
            'neuromorpho': r'neuromorpho\.org',
            'huggingface': r'huggingface\.co',
            'codeocean': r'codeocean\.com',
            'mendeley data': r'data\.mendeley\.com',
            'synapse': r'synapse\.org'
        }

        # DOI模式
        self.doi_patterns = [
            r'doi\.org\/([^\s\"\'<>]+)',
            r'doi:[^\s\"\'<>]+',
            r'digital\s+object\s+identifier[:\s]+([^\s\"\'<>]+)',
        ]

        # Accession number模式
        self.accession_patterns = [
            r'accession\s+(?:code|number)[:\s]+([a-zA-Z0-9._-]+)',
            r'accession[:\s]+([a-zA-Z0-9._-]+)',
            r'(?:GEO|SRA|ENA|DDBJ|ArrayExpress|BioSample|BioProject)\s+accession[:\s]+([a-zA-Z0-9._-]+)',
            r'(?:GEO|SRA|ENA|DDBJ|ArrayExpress)\s*:\s*([a-zA-Z0-9._-]+)',
            r'(?:GSE|GSM|SRP|SRR|ERP|ERR|DRP|DRR|PRJNA|SAMN)[0-9]{5,}',  # 通用的Accession number模式
            r'(?:E-[A-Z]{3,4}-[0-9]+)'  # ArrayExpress格式
        ]

        # 神经科学数据描述模式
        self.neuro_data_patterns = [
            r'(?:calcium|voltage|neural|neuron|brain|cortex|fMRI|EEG|MEG)\s+(?:imaging|recording|data|dataset)',
            r'(?:neuron|neural|brain|neuronal|cortical)\s+(?:activity|recording|firing|spike|action potential)',
            r'(?:single-cell|single cell|spatial)\s+(?:RNA-seq|RNA sequencing|transcriptomics|gene expression)',
            r'(?:patch clamp|voltage clamp|current clamp|electrophysiology|whole-cell recording)',
            r'(?:electron|confocal|two-photon|multiphoton|super-resolution)\s+microscopy',
            r'(?:circuit reconstruction|connectome|tractography|connectomics)',
            r'(?:diffusion MRI|DTI|diffusion tensor|tractography)',
            r'(?:neuroanatomy|brain atlas|brain mapping)'
        ]

    def extract_from_html(self, html_content, article_url, journal_type='nature'):
        """
        从HTML内容中提取数据集信息

        Args:
            html_content (str): HTML内容
            article_url (str): 文章URL，用于构建相对URL
            journal_type (str): 期刊类型，用于选择不同的选择器

        Returns:
            list: 提取到的数据集列表
        """
        if not html_content:
            return []

        datasets = []

        # 选择合适的选择器集
        if journal_type == 'nature':
            selectors = NATURE_SELECTORS
        elif journal_type == 'science':
            selectors = SCIENCE_SELECTORS
        elif journal_type == 'cell':
            selectors = CELL_SELECTORS
        else:
            logger.warning(f"未知的期刊类型: {journal_type}，使用Nature选择器")
            selectors = NATURE_SELECTORS

        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # 1. 查找数据可用性部分
            data_availability_section = None
            for selector in selectors['data_availability']:
                sections = soup.select(selector)
                if sections:
                    data_availability_section = sections[0]
                    break

            # 2. 如果找到数据可用性部分，提取其中的数据集信息
            data_links = []
            data_text = ""

            if data_availability_section:
                data_text = data_availability_section.text
                data_links = data_availability_section.select('a')

                # 从链接中提取数据集
                datasets.extend(self._extract_from_links(data_links, article_url))

                # 从文本中提取数据集信息
                datasets.extend(self._extract_from_text(data_text, article_url))

            # 3. 尝试从整个文档中查找数据集引用
            # 查找所有可能包含数据集引用的段落
            data_related_paragraphs = []

            # 关键词搜索: 寻找包含数据、代码、可用性等关键词的段落
            keywords = ['data', 'dataset', 'code', 'software', 'availability',
                        'repository', 'accession', 'github', 'zenodo', 'figshare']

            for p in soup.find_all(['p', 'div']):
                text = p.text.lower()
                if any(kw in text for kw in keywords):
                    data_related_paragraphs.append(p)

            # 从这些段落中提取链接和文本
            for p in data_related_paragraphs:
                links = p.select('a')
                datasets.extend(self._extract_from_links(links, article_url))
                datasets.extend(self._extract_from_text(p.text, article_url))

            # 4. 查找补充材料部分
            supp_links = []
            for selector in selectors.get('supplementary', []):
                supp_elements = soup.select(selector)
                if supp_elements:
                    supp_links.extend(supp_elements)

            for link in supp_links:
                href = link.get('href')
                if href:
                    datasets.append({
                        'name': link.text.strip() or "Supplementary Materials",
                        'url': urljoin(article_url, href),
                        'repository': 'journal_supplementary',
                        'source': journal_type
                    })

            # 5. 去重
            return self._deduplicate_datasets(datasets)

        except Exception as e:
            logger.error(f"提取数据集信息时出错: {e}, url: {article_url}")
            return datasets

    def _extract_from_links(self, links, base_url):
        """从链接元素中提取数据集信息"""
        datasets = []

        for link in links:
            try:
                link_url = link.get('href', '')
                if not link_url:
                    continue

                # 确保URL是完整的
                if not link_url.startswith(('http://', 'https://')):
                    link_url = urljoin(base_url, link_url)

                link_text = link.text.strip()

                # 识别数据仓库
                repository_name = self._identify_repository(link_url, link_text)

                # 如果识别出了数据仓库，添加到数据集列表
                if repository_name:
                    dataset = {
                        'name': link_text if link_text else f"Dataset from {repository_name}",
                        'url': link_url,
                        'repository': repository_name,
                        'found_in': 'link'
                    }
                    datasets.append(dataset)

                # 即使没有识别出仓库，如果链接文本暗示这是数据集，也添加
                elif any(kw in link_text.lower() for kw in ['data', 'dataset', 'code', 'software', 'repository']):
                    # 解析域名作为可能的仓库
                    domain = urlparse(link_url).netloc
                    dataset = {
                        'name': link_text,
                        'url': link_url,
                        'repository': domain,
                        'found_in': 'link_text_keyword'
                    }
                    datasets.append(dataset)

            except Exception as e:
                logger.error(f"处理链接时出错: {e}")

        return datasets

    def _extract_from_text(self, text, base_url=None):
        """从文本中提取数据集信息"""
        datasets = []

        # 查找DOI引用
        for pattern in self.doi_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                doi = match.strip()
                if doi:
                    # 规范化DOI
                    if doi.startswith('doi:'):
                        doi = doi[4:]

                    dataset = {
                        'name': f"Dataset DOI: {doi}",
                        'url': f"https://doi.org/{doi}" if not doi.startswith('http') else doi,
                        'repository': 'DOI',
                        'found_in': 'text_doi'
                    }
                    datasets.append(dataset)

        # 查找Accession number
        for pattern in self.accession_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    match = match[0]  # 如果是捕获组

                accession = match.strip()
                if accession:
                    # 尝试确定数据库类型
                    db_type = 'unknown'
                    if re.match(r'GSE\d+', accession):
                        db_type = 'GEO Series'
                        url = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={accession}"
                    elif re.match(r'GSM\d+', accession):
                        db_type = 'GEO Sample'
                        url = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={accession}"
                    elif re.match(r'SRP\d+|SRR\d+', accession):
                        db_type = 'SRA'
                        url = f"https://www.ncbi.nlm.nih.gov/sra/{accession}"
                    elif re.match(r'PRJNA\d+', accession):
                        db_type = 'BioProject'
                        url = f"https://www.ncbi.nlm.nih.gov/bioproject/{accession}"
                    elif re.match(r'SAMN\d+', accession):
                        db_type = 'BioSample'
                        url = f"https://www.ncbi.nlm.nih.gov/biosample/{accession}"
                    elif re.match(r'E-[A-Z]{3,4}-\d+', accession):
                        db_type = 'ArrayExpress'
                        url = f"https://www.ebi.ac.uk/arrayexpress/experiments/{accession}"
                    else:
                        url = None

                    dataset = {
                        'name': f"Dataset Accession: {accession}",
                        'url': url,
                        'repository': db_type,
                        'accession': accession,
                        'found_in': 'text_accession'
                    }
                    datasets.append(dataset)

        # 查找URL引用 (不是DOI的URL)
        url_pattern = r'(https?://(?!dx\.doi\.org)[^\s\"\'<>]+)'
        urls = re.findall(url_pattern, text)

        for url in urls:
            # 检查是否是数据仓库URL
            repository_name = self._identify_repository(url)
            if repository_name:
                dataset = {
                    'name': f"Dataset from {repository_name}",
                    'url': url,
                    'repository': repository_name,
                    'found_in': 'text_url'
                }
                datasets.append(dataset)

        return datasets

    def _identify_repository(self, url, text=''):
        """识别URL或文本对应的数据仓库"""
        combined_text = f"{url} {text}".lower()

        for repo_name, pattern in self.data_repositories.items():
            if re.search(pattern, combined_text, re.IGNORECASE):
                return repo_name

        return None

    def _deduplicate_datasets(self, datasets):
        """去除重复的数据集条目"""
        unique_datasets = []
        seen_urls = set()
        seen_accessions = set()

        for dataset in datasets:
            # 检查URL唯一性
            url = dataset.get('url')
            accession = dataset.get('accession')

            # 如果有URL且不重复
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_datasets.append(dataset)
            # 如果有accession且不重复
            elif accession and accession not in seen_accessions and not url:
                seen_accessions.add(accession)
                unique_datasets.append(dataset)

        return unique_datasets

    def identify_data_types(self, text):
        """识别文本中提及的神经科学数据类型"""
        data_types = set()

        if not text:
            return []

        text = text.lower()

        # 神经元图像数据
        if re.search(r'neuron\s+imaging|neuron\s+morphology|calcium\s+imaging|'
                     r'neuronal\s+activity|fluorescence|two-photon|gcamp|microscopy', text):
            data_types.add("neuron_imaging")

        # 重建数据
        if re.search(r'reconstruction|3d\s+reconstruction|connectome|'
                     r'neuronal\s+circuit|circuit\s+reconstruction', text):
            data_types.add("reconstruction")

        # 空间转录组数据
        if re.search(r'spatial\s+transcriptomics|single[\s-]cell\s+rna[\s-]seq|scrna[\s-]seq|'
                     r'spatial\s+gene\s+expression|spatial\s+omics', text):
            data_types.add("spatial_transcriptomics")

        # MRI数据
        if re.search(r'mri|fmri|magnetic\s+resonance\s+imaging|diffusion\s+mri|'
                     r'brain\s+imaging|tractography', text):
            data_types.add("mri")

        # 电生理数据
        if re.search(r'electrophysiology|patch\s+clamp|spike\s+sorting|eeg|'
                     r'meg|lfp|action\s+potential|ephys', text):
            data_types.add("electrophysiology")

        # 行为数据
        if re.search(r'behavioral\s+data|behaviour|behavior\s+test|behavioral\s+paradigm|'
                     r'mouse\s+behavior|animal\s+behavior', text):
            data_types.add("behavioral")

        # 组织学数据
        if re.search(r'histology|immunohistochemistry|immunofluorescence|'
                     r'tissue\s+section|staining', text):
            data_types.add("histology")

        return list(data_types)