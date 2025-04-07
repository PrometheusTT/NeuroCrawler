#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import logging
import nltk
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from collections import Counter
import string

logger = logging.getLogger(__name__)

# 尝试下载NLTK数据，如果已下载则跳过
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')


def is_neuroscience_related(text):
    """
    判断文本是否与神经科学相关

    Args:
        text (str): 要分析的文本

    Returns:
        bool: 是否与神经科学相关
    """
    if not text:
        return False

    # 神经科学关键词
    neuroscience_keywords = [
        "neuroscience", "neural", "brain", "neuron", "cortex",
        "cognition", "cognitive", "neuroimaging", "connectome",
        "neuroinformatics", "computational neuroscience", "synaptic",
        "dendrite", "axon", "hippocampus", "amygdala", "prefrontal",
        "cerebellum", "thalamus", "neurotransmitter", "dopamine",
        "serotonin", "glutamate", "gaba", "action potential",
        "spike", "firing rate", "local field potential", "eeg", "meg",
        "fmri", "bold", "dti", "diffusion tensor", "connectomics",
        "neural network", "deep learning", "machine learning",
        "artificial intelligence", "brain-computer interface",
        "optogenetics", "calcium imaging", "patch clamp",
        "whole-cell recording", "extracellular recording"
    ]

    # 将文本转为小写并检查关键词是否出现
    text_lower = text.lower()

    # 计算包含神经科学关键词的数量
    keyword_count = sum(1 for keyword in neuroscience_keywords if keyword in text_lower)

    # 如果包含超过2个神经科学关键词，认为与神经科学相关
    return keyword_count >= 2


def extract_keywords(text, top_n=10):
    """
    从文本中提取关键词

    Args:
        text (str): 要分析的文本
        top_n (int): 返回的关键词数量

    Returns:
        list: 关键词列表
    """
    if not text:
        return []

    # 转为小写
    text = text.lower()

    # 标点符号和停用词
    punctuation = string.punctuation
    stop_words = set(stopwords.words('english'))

    # 分词
    words = word_tokenize(text)

    # 过滤掉标点符号和停用词
    filtered_words = [
        word for word in words
        if word not in punctuation
           and word not in stop_words
           and len(word) > 2  # 过滤短词
    ]

    # 统计词频
    word_counts = Counter(filtered_words)

    # 返回出现频率最高的top_n个词
    return [word for word, count in word_counts.most_common(top_n)]


def extract_dataset_links(text):
    """
    从文本中提取可能的数据集链接或引用

    Args:
        text (str): 要分析的文本

    Returns:
        list: 可能的数据集链接或引用
    """
    if not text:
        return []

    dataset_references = []

    # 1. 查找URL
    url_pattern = r'https?://[^\s<>"\']+[^\s<>"\',\.]'
    urls = re.findall(url_pattern, text)

    # 过滤可能的数据集链接
    data_repo_keywords = [
        'figshare', 'zenodo', 'dryad', 'osf.io', 'github', 'ncbi', 'ebi',
        'geo', 'genbank', 'sra', 'dataset', 'data.', 'neurodata', 'neurovault',
        'openneuro', 'brainmaps', 'brain-map', 'humanconnectome', 'ukbiobank'
    ]

    for url in urls:
        if any(keyword in url.lower() for keyword in data_repo_keywords):
            dataset_references.append({'type': 'url', 'value': url})

    # 2. 查找DOI
    doi_pattern = r'doi:([^\s]+)|https?://doi\.org/([^\s]+)'
    doi_matches = re.findall(doi_pattern, text)

    for match in doi_matches:
        doi = match[0] if match[0] else match[1]
        if doi:
            dataset_references.append({'type': 'doi', 'value': doi})

    # 3. 查找Accession numbers
    accession_patterns = [
        r'accession\s+(?:code|number)[:\s]+([a-zA-Z0-9]+)',
        r'accession[:\s]+([a-zA-Z0-9]+)',
        r'(?:GEO|SRA|ENA|DDBJ|ArrayExpress|BioSample|BioProject)\s+accession[:\s]+([a-zA-Z0-9]+)',
        r'(?:GSE|GSM|SRP|SRR|ERP|ERR|DRP|DRR|PRJNA|SAMN)[0-9]{5,}',
        r'(?:E-[A-Z]{3,4}-[0-9]+)'
    ]

    for pattern in accession_patterns:
        accession_matches = re.findall(pattern, text)
        for match in accession_matches:
            if match:
                dataset_references.append({'type': 'accession', 'value': match})

    # 4. 查找包含"data available at"或类似表述的句子
    sentences = sent_tokenize(text)
    data_phrases = [
        'data available at', 'data are available at',
        'dataset is available', 'data can be accessed',
        'data can be found', 'data is deposited',
        'data are deposited', 'code is available'
    ]

    for sentence in sentences:
        if any(phrase in sentence.lower() for phrase in data_phrases):
            dataset_references.append({'type': 'sentence', 'value': sentence.strip()})

    return dataset_references