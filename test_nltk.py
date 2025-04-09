#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import ssl
import nltk
import certifi

print("===== NLTK 数据下载修复工具 =====")

# 方法1: 使用 certifi 证书
try:
    print("尝试方法1: 使用 certifi 提供的证书...")
    os.environ['SSL_CERT_FILE'] = certifi.where()
    os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()

    print(f"证书路径设置为: {certifi.where()}")
    nltk.download('punkt')
    nltk.download('stopwords')
    print("方法1成功!")
    exit(0)
except Exception as e:
    print(f"方法1失败: {e}")

# 方法2: 禁用证书验证
try:
    print("\n尝试方法2: 临时禁用证书验证...")
    _create_unverified_https_context = ssl._create_unverified_context
    ssl._create_default_https_context = _create_unverified_https_context

    # 设置 NLTK 数据目录
    nltk_data_dir = os.path.expanduser('~/nltk_data')
    os.makedirs(nltk_data_dir, exist_ok=True)
    nltk.data.path.append(nltk_data_dir)

    print(f"NLTK 数据将保存到: {nltk_data_dir}")
    nltk.download('punkt', download_dir=nltk_data_dir)
    nltk.download('stopwords', download_dir=nltk_data_dir)
    print("方法2成功!")
    exit(0)
except Exception as e:
    print(f"方法2失败: {e}")

# 方法3: 手动下载
print("\n尝试方法3: 提供手动下载指引...")
print("""
请手动下载以下文件:
1. punkt 分词模型:
   https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/tokenizers/punkt.zip

2. stopwords 停用词:
   https://raw.githubusercontent.com/nltk/nltk_data/gh-pages/packages/corpora/stopwords.zip

下载后将它们放到以下目录:
- punkt 解压到 ~/nltk_data/tokenizers/
- stopwords 解压到 ~/nltk_data/corpora/
""")