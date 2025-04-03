#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import re
import string
from collections import Counter

logger = logging.getLogger(__name__)


class NLPTools:
    """
    自然语言处理工具，用于文本分析、关键词提取等
    """

    def __init__(self):
        # 神经科学领域常见术语
        self.neuroscience_terms = [
            "neuroscience", "neural", "brain", "neuron", "cortex",
            "cognition", "cognitive", "eeg", "fmri", "meg", "spike",
            "action potential", "neuroimaging", "connectome", "synaptic",
            "axon", "dendrite", "hippocampus", "amygdala", "prefrontal",
            "thalamus", "basal ganglia", "cerebellum", "brain-computer interface",
            "neural network", "deep learning", "computational neuroscience",
            "electrophysiology", "optogenetics", "calcium imaging",
            "patch clamp", "transcriptomics", "spike sorting", "neurotransmitter"
        ]

        # 常见停用词
        self.stopwords = set([
            "a", "an", "the", "and", "or", "but", "if", "because", "as", "what",
            "which", "this", "that", "these", "those", "then", "just", "so", "than",
            "such", "both", "through", "about", "for", "is", "of", "while", "during",
            "to", "from", "in", "on", "by", "with", "at", "into", "only", "few",
            "some", "many", "most", "other", "such", "no", "nor", "not", "can", "will",
            "don't", "doesn't", "didn't", "won't", "wouldn't", "shouldn't", "couldn't"
        ])

    def is_neuroscience_related(self, text):
        """
        判断文本是否与神经科学相关
        基于关键词匹配和内容分析
        """
        if not text:
            return False

        # 转为小写进行匹配
        text_lower = text.lower()

        # 检查是否包含神经科学术语
        for term in self.neuroscience_terms:
            if term.lower() in text_lower:
                return True

        # 进一步分析文本内容
        # 提取关键词并计算相关性分数
        keywords = self.extract_keywords(text, n=20)

        # 计算相关性分数
        score = sum(1 for word in keywords if any(
            term.lower() in word.lower() or word.lower() in term.lower()
            for term in self.neuroscience_terms
        ))

        # 如果相关性分数超过阈值，判断为相关
        return score >= 3

    def extract_keywords(self, text, n=10):
        """
        从文本中提取关键词
        使用TF-IDF思想的简化版本
        """
        if not text:
            return []

        # 文本预处理
        text = text.lower()

        # 移除标点符号
        text = text.translate(str.maketrans('', '', string.punctuation))

        # 分词
        words = text.split()

        # 移除停用词
        words = [word for word in words if word not in self.stopwords]

        # 计算词频
        word_counts = Counter(words)

        # 过滤掉单个字符和纯数字
        word_counts = {k: v for k, v in word_counts.items()
                       if len(k) > 1 and not k.isdigit()}

        # 获取前n个关键词
        keywords = [word for word, _ in word_counts.most_common(n)]

        return keywords

    def extract_github_links(self, text):
        """从文本中提取GitHub仓库链接"""
        if not text:
            return []

        # 匹配GitHub链接
        github_pattern = r'https?://github\.com/([a-zA-Z0-9-]+)/([a-zA-Z0-9_.-]+)'
        matches = re.findall(github_pattern, text)

        return [{'user': match[0], 'repo': match[1]} for match in matches]

    def extract_dataset_links(self, text):
        """从文本中提取数据集链接"""
        if not text:
            return []

        # 常见数据集托管平台
        platforms = [
            {'name': 'figshare', 'pattern': r'https?://(?:www\.)?figshare\.com/[^\s]+'},
            {'name': 'zenodo', 'pattern': r'https?://(?:www\.)?zenodo\.org/[^\s]+'},
            {'name': 'dataverse', 'pattern': r'https?://(?:www\.)?dataverse\.harvard\.edu/[^\s]+'},
            {'name': 'dryad', 'pattern': r'https?://(?:www\.)?datadryad\.org/[^\s]+'},
            {'name': 'osf', 'pattern': r'https?://(?:www\.)?osf\.io/[^\s]+'},
            {'name': 'kaggle', 'pattern': r'https?://(?:www\.)?kaggle\.com/datasets/[^\s]+'},
            {'name': 'openneuro', 'pattern': r'https?://(?:www\.)?openneuro\.org/[^\s]+'},
            {'name': 'neurodata', 'pattern': r'https?://(?:www\.)?neurodata\.io/[^\s]+'},
            {'name': 'crcns', 'pattern': r'https?://(?:www\.)?crcns\.org/[^\s]+'},
            {'name': 'gin', 'pattern': r'https?://(?:www\.)?gin\.g-node\.org/[^\s]+'}
        ]

        results = []

        # 查找每个平台的链接
        for platform in platforms:
            matches = re.findall(platform['pattern'], text)
            for url in matches:
                results.append({
                    'platform': platform['name'],
                    'url': url
                })

        # 查找DOI链接
        doi_pattern = r'https?://(?:www\.)?doi\.org/([^\s]+)'
        doi_matches = re.findall(doi_pattern, text)
        for doi in doi_matches:
            results.append({
                'platform': 'doi',
                'url': f'https://doi.org/{doi}',
                'doi': doi
            })

        return results


# 初始化单例实例供外部使用
nlp = NLPTools()


# 导出简化函数
def is_neuroscience_related(text):
    return nlp.is_neuroscience_related(text)


def extract_keywords(text, n=10):
    return nlp.extract_keywords(text, n)


def extract_github_links(text):
    return nlp.extract_github_links(text)


def extract_dataset_links(text):
    return nlp.extract_dataset_links(text)