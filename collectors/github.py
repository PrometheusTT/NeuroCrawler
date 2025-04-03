#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import time
import re
import random
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from utils.proxy_manager import ProxyManager
from utils.browser_emulator import BrowserEmulator

logger = logging.getLogger(__name__)


class GitHubCollector:
    """
    用于爬取神经科学相关GitHub仓库的爬虫
    1. 从论文中提取GitHub链接
    2. 根据关键词搜索GitHub
    """

    def __init__(self, config):
        self.config = config
        self.api_url = "https://api.github.com"
        self.search_url = "https://github.com/search"
        self.proxy_manager = ProxyManager()
        self.browser = BrowserEmulator()
        self.github_tokens = config.get('api_tokens', [])
        self.current_token_index = 0

        # 神经科学相关的GitHub搜索关键词
        self.neuroscience_keywords = [
            "neuroscience", "neural-network", "brain-model", "neuroimaging",
            "connectome", "spike-sorting", "eeg-analysis", "fmri-analysis",
            "computational-neuroscience", "neural-data", "brain-computer-interface"
        ]

        # GitHub仓库质量评估指标
        self.quality_metrics = {
            'stars_threshold': 5,  # 最低star数
            'commits_threshold': 10,  # 最低提交数
            'last_update_days': 365,  # 最近更新时间(天)
            'readme_required': True,  # 是否要求有README
            'license_required': False  # 是否要求有许可证
        }

    def _get_next_token(self):
        """轮换使用GitHub API令牌以避免触发频率限制"""
        if not self.github_tokens:
            return None

        token = self.github_tokens[self.current_token_index]
        self.current_token_index = (self.current_token_index + 1) % len(self.github_tokens)
        return token

    def _make_api_request(self, endpoint, params=None):
        """向GitHub API发出请求"""
        url = f"{self.api_url}{endpoint}"
        token = self._get_next_token()
        headers = {
            'User-Agent': 'NeuroCrawler/1.0',
            'Accept': 'application/vnd.github.v3+json'
        }

        if token:
            headers['Authorization'] = f"token {token}"

        try:
            proxy = self.proxy_manager.get_proxy()
            response = requests.get(
                url,
                headers=headers,
                params=params,
                proxies=proxy,
                timeout=30
            )

            # 检查是否接近API限制
            if 'X-RateLimit-Remaining' in response.headers:
                remaining = int(response.headers['X-RateLimit-Remaining'])
                if remaining < 10:
                    logger.warning(f"GitHub API限额即将用尽, 剩余: {remaining}")

            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"GitHub API请求失败: {response.status_code}, {response.text}")
                return None

        except Exception as e:
            logger.error(f"GitHub API请求出错: {e}")
            return None

    def _extract_github_links(self, papers):
        """从论文内容中提取GitHub链接"""
        github_links = []

        for paper in papers:
            # 从标题、摘要和全文中提取链接
            text = paper.get('title', '') + ' ' + paper.get('abstract', '')

            # 如果有原始数据，也从中提取
            if 'raw_data' in paper:
                if hasattr(paper['raw_data'], 'get'):
                    text += str(paper['raw_data'].get('text', ''))
                else:
                    text += str(paper['raw_data'])

            # 提取GitHub链接
            # 匹配模式: github.com/user/repo 或 github.com/user/repo/
            github_patterns = [
                r'github\.com/([a-zA-Z0-9-]+)/([a-zA-Z0-9_.-]+)',
                r'github\.com/([a-zA-Z0-9-]+)/([a-zA-Z0-9_.-]+)/'
            ]

            for pattern in github_patterns:
                matches = re.findall(pattern, text)
                for match in matches:
                    if len(match) >= 2:
                        user, repo = match[0], match[1]
                        github_links.append({
                            'user': user,
                            'repo': repo,
                            'paper_id': paper.get('id'),
                            'paper_title': paper.get('title')
                        })

        return github_links

    def _search_repositories(self, keywords, sort_by='stars', order='desc', per_page=30):
        """使用关键词搜索GitHub仓库"""
        repositories = []

        for keyword in keywords:
            try:
                # 使用API搜索
                params = {
                    'q': f"{keyword} in:name,description,readme",
                    'sort': sort_by,
                    'order': order,
                    'per_page': per_page
                }

                search_results = self._make_api_request('/search/repositories', params)

                if search_results and 'items' in search_results:
                    for repo in search_results['items']:
                        repositories.append({
                            'user': repo['owner']['login'],
                            'repo': repo['name'],
                            'keyword': keyword
                        })

                # 避免触发频率限制
                time.sleep(random.uniform(2, 5))

            except Exception as e:
                logger.error(f"搜索GitHub仓库时出错: {e}, keyword: {keyword}")

        return repositories

    def _get_repository_info(self, user, repo):
        """获取GitHub仓库的详细信息"""
        try:
            repo_info = self._make_api_request(f'/repos/{user}/{repo}')

            if not repo_info:
                return None

            # 获取最近的提交
            commits = self._make_api_request(f'/repos/{user}/{repo}/commits', {'per_page': 1})

            # 检查是否符合质量标准
            if repo_info['stargazers_count'] < self.quality_metrics['stars_threshold']:
                return None

            # 检查最后更新时间
            last_update = datetime.strptime(repo_info['updated_at'], "%Y-%m-%dT%H:%M:%SZ")
            if (datetime.now() - last_update).days > self.quality_metrics['last_update_days']:
                return None

            # 构建仓库信息
            repository = {
                'user': user,
                'repo': repo,
                'full_name': repo_info['full_name'],
                'url': repo_info['html_url'],
                'description': repo_info['description'],
                'stars': repo_info['stargazers_count'],
                'forks': repo_info['forks_count'],
                'watchers': repo_info['watchers_count'],
                'language': repo_info['language'],
                'created_at': repo_info['created_at'],
                'updated_at': repo_info['updated_at'],
                'topics': repo_info.get('topics', []),
                'has_readme': None,  # 将在下一步检查
                'has_license': repo_info['license'] is not None,
                'last_commit': commits[0]['sha'] if commits else None,
                'last_commit_date': commits[0]['commit']['author']['date'] if commits else None
            }

            # 检查是否有README
            readme = self._make_api_request(f'/repos/{user}/{repo}/readme')
            repository['has_readme'] = readme is not None

            # 如果要求有README但没有，则跳过
            if self.quality_metrics['readme_required'] and not repository['has_readme']:
                return None

            # 如果要求有许可证但没有，则跳过
            if self.quality_metrics['license_required'] and not repository['has_license']:
                return None

            return repository

        except Exception as e:
            logger.error(f"获取仓库信息时出错: {e}, repo: {user}/{repo}")
            return None

    def collect_repositories(self, papers, include_search=True):
        """收集与论文相关的GitHub仓库"""
        repositories = []

        # 从论文中提取GitHub链接
        logger.info("从论文中提取GitHub链接")
        github_links = self._extract_github_links(papers)

        # 获取每个仓库的详细信息
        for link in github_links:
            logger.info(f"获取仓库信息: {link['user']}/{link['repo']}")
            repo_info = self._get_repository_info(link['user'], link['repo'])

            if repo_info:
                # 添加论文引用信息
                repo_info['source'] = 'paper_mention'
                repo_info['referenced_in'] = {
                    'paper_id': link.get('paper_id'),
                    'paper_title': link.get('paper_title')
                }
                repositories.append(repo_info)

            # 避免触发频率限制
            time.sleep(random.uniform(1, 3))

        # 如果启用了搜索功能，还可以根据神经科学关键词搜索GitHub仓库
        if include_search:
            logger.info("根据关键词搜索GitHub仓库")
            search_results = self._search_repositories(self.neuroscience_keywords)

            for result in search_results:
                # 检查是否已经添加过
                if any(r['user'] == result['user'] and r['repo'] == result['repo'] for r in repositories):
                    continue

                repo_info = self._get_repository_info(result['user'], result['repo'])

                if repo_info:
                    # 添加搜索信息
                    repo_info['source'] = 'keyword_search'
                    repo_info['keyword'] = result['keyword']
                    repositories.append(repo_info)

                # 避免触发频率限制
                time.sleep(random.uniform(1, 3))

        logger.info(f"共收集了 {len(repositories)} 个GitHub仓库")
        return repositories