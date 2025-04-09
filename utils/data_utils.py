#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import logging
import requests
import shutil
import tempfile
import json
import time
from urllib.parse import urlparse, unquote
from pathlib import Path

logger = logging.getLogger(__name__)


class DataDownloader:
    """通用数据下载器，支持多种数据源和下载方式"""

    def __init__(self, config=None):
        """
        初始化下载器

        Args:
            config: 下载器配置
        """
        self.config = config or {}
        self.download_dir = self.config.get('download_dir', 'downloads')
        self.timeout = self.config.get('timeout', 300)  # 下载超时时间（秒）
        self.retry_count = self.config.get('retry_count', 3)  # 重试次数
        self.delay_between_retry = self.config.get('delay_between_retry', 5)  # 重试间隔（秒）
        self.skip_existing = self.config.get('skip_existing', True)  # 跳过已存在的文件

        # 确保下载目录存在
        os.makedirs(self.download_dir, exist_ok=True)

        # 下载记录文件
        self.download_history_file = os.path.join(self.download_dir, 'download_history.json')
        self.download_history = self._load_download_history()

    def _load_download_history(self):
        """加载下载历史记录"""
        if os.path.exists(self.download_history_file):
            try:
                with open(self.download_history_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"加载下载历史记录失败: {e}")
                return {}
        return {}

    def _save_download_history(self):
        """保存下载历史记录"""
        try:
            with open(self.download_history_file, 'w', encoding='utf-8') as f:
                json.dump(self.download_history, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"保存下载历史记录失败: {e}")

    def is_dataset_downloaded(self, dataset):
        """
        检查数据集是否已下载

        Args:
            dataset: 数据集信息

        Returns:
            bool: 是否已下载
        """
        # 确保URL是字符串
        url = str(dataset.get('url', ''))
        if not url:
            return False

        # 检查下载历史
        download_id = self._get_download_id(dataset)
        if download_id in self.download_history:
            return self.download_history[download_id].get('status') == 'success'

        return False

    def _get_download_id(self, dataset):
        """获取数据集的唯一标识"""
        # 确保字段是字符串
        url = str(dataset.get('url', ''))
        name = str(dataset.get('name', ''))
        repository = str(dataset.get('repository', ''))

        if url:
            return f"url:{url}"
        elif name and repository:
            return f"name:{name}|repo:{repository}"
        else:
            # 生成唯一ID
            import hashlib
            content = json.dumps(dataset, sort_keys=True)
            return f"hash:{hashlib.md5(content.encode()).hexdigest()}"

    def download_dataset(self, dataset, target_dir=None):
        """
        下载单个数据集

        Args:
            dataset: 数据集信息字典
            target_dir: 目标保存目录

        Returns:
            bool: 成功返回True，失败返回False
            str: 错误信息（如果有）
        """
        try:
            # 确保URL是字符串类型
            if 'url' not in dataset:
                return False, "数据集缺少URL信息"

            url = str(dataset['url'])  # 确保URL是字符串

            # 获取数据集名称（确保转为字符串）
            name = str(dataset.get('name', 'unnamed_dataset'))
            repository = str(dataset.get('repository', 'unknown'))

            # 确保目标目录存在
            if target_dir is None:
                target_dir = self.download_dir

            # 创建特定于这个数据集的目录
            dataset_dir = os.path.join(target_dir, self._sanitize_filename(f"{repository}_{name}"))
            os.makedirs(dataset_dir, exist_ok=True)

            # 检查该数据集是否已下载
            download_id = self._get_download_id(dataset)
            if download_id in self.download_history and self.download_history[download_id].get('status') == 'success':
                if self.skip_existing:
                    return True, "已下载"

            # 检查URL类型
            url_type = self._determine_url_type(url)

            # 根据不同类型的URL使用不同的下载方法
            if url_type == 'direct_download':
                success, message = self._download_direct_file(url, dataset_dir, name)
            elif url_type == 'github':
                success, message = self._download_github(url, dataset_dir)
            elif url_type == 'figshare':
                success, message = self._download_figshare(url, dataset_dir)
            elif url_type == 'zenodo':
                success, message = self._download_zenodo(url, dataset_dir)
            elif url_type == 'osf':
                success, message = self._download_osf(url, dataset_dir)
            elif url_type == 'dryad':
                success, message = self._download_dryad(url, dataset_dir)
            elif url_type == 'dataverse':
                success, message = self._download_dataverse(url, dataset_dir)
            elif url_type == 'kaggle':
                success, message = self._download_kaggle(url, dataset_dir)
            else:
                # 默认作为网页处理
                success, message = self._download_webpage(url, dataset_dir)

            # 更新下载历史
            self.download_history[download_id] = {
                'url': url,
                'name': name,
                'repository': repository,
                'download_time': time.strftime('%Y-%m-%d %H:%M:%S'),
                'status': 'success' if success else 'failed',
                'message': message,
                'path': dataset_dir if success else None
            }
            self._save_download_history()

            return success, message

        except Exception as e:
            logger.error(f"下载数据集时出错: {e}")
            return False, str(e)

    def download_datasets(self, datasets):
        """
        批量下载数据集

        Args:
            datasets: 数据集列表

        Returns:
            dict: 下载结果统计
        """
        total = len(datasets)
        success_count = 0
        failed_count = 0
        skipped_count = 0
        details = []

        logger.info(f"开始下载 {total} 个数据集")

        try:
            for dataset in datasets:
                try:
                    # 验证数据集URL
                    if 'url' not in dataset or not dataset['url']:
                        logger.warning(f"跳过没有URL的数据集: {dataset.get('name', 'Unknown')}")
                        skipped_count += 1
                        continue

                    # 确保所有需要字符串的字段都是字符串类型
                    dataset_sanitized = {
                        key: str(value) if key in ['url', 'name', 'repository', 'paper_title',
                                                   'paper_url'] and value is not None else value
                        for key, value in dataset.items()
                    }

                    # 检查该数据集是否已下载
                    if self.is_dataset_downloaded(dataset_sanitized):
                        logger.info(f"数据集已存在，跳过: {dataset_sanitized.get('name', 'Unknown')}")
                        skipped_count += 1
                        details.append({
                            'dataset': dataset_sanitized.get('name', 'Unknown'),
                            'repository': dataset_sanitized.get('repository', 'Unknown'),
                            'success': False,
                            'skipped': True,
                            'error': '已存在'
                        })
                        continue

                    # 下载数据集
                    success, error = self.download_dataset(dataset_sanitized)

                    if success:
                        logger.info(f"成功下载数据集: {dataset_sanitized.get('name', 'Unknown')}")
                        success_count += 1
                        details.append({
                            'dataset': dataset_sanitized.get('name', 'Unknown'),
                            'repository': dataset_sanitized.get('repository', 'Unknown'),
                            'success': True
                        })
                    else:
                        logger.error(f"下载数据集失败: {dataset_sanitized.get('name', 'Unknown')}, 错误: {error}")
                        failed_count += 1
                        details.append({
                            'dataset': dataset_sanitized.get('name', 'Unknown'),
                            'repository': dataset_sanitized.get('repository', 'Unknown'),
                            'success': False,
                            'error': error
                        })

                except Exception as e:
                    logger.error(f"处理数据集下载时出错: {e}")
                    failed_count += 1
                    details.append({
                        'dataset': dataset.get('name', 'Unknown'),
                        'repository': dataset.get('repository', 'Unknown'),
                        'success': False,
                        'error': str(e)
                    })

            return {
                'total': total,
                'success': success_count,
                'failed': failed_count,
                'skipped': skipped_count,
                'details': details
            }

        except Exception as e:
            logger.error(f"批量下载数据集时出错: {e}")
            return {
                'total': total,
                'success': success_count,
                'failed': failed_count,
                'skipped': skipped_count,
                'details': details
            }

    def _determine_url_type(self, url):
        """
        确定URL类型

        Args:
            url: URL字符串

        Returns:
            str: URL类型
        """
        url = str(url).lower()

        # 检查是否是直接下载链接
        file_extensions = ['.zip', '.tar', '.gz', '.rar', '.7z', '.csv', '.txt', '.json', '.pdf', '.nii', '.nii.gz',
                           '.mat']
        if any(url.endswith(ext) for ext in file_extensions):
            return 'direct_download'

        # 检查是否是特定平台
        if 'github.com' in url:
            return 'github'
        elif 'figshare.com' in url:
            return 'figshare'
        elif 'zenodo.org' in url:
            return 'zenodo'
        elif 'osf.io' in url:
            return 'osf'
        elif 'datadryad.org' in url:
            return 'dryad'
        elif 'dataverse' in url:
            return 'dataverse'
        elif 'kaggle.com' in url:
            return 'kaggle'

        # 默认类型
        return 'webpage'

    def _download_with_retry(self, url, local_path, headers=None):
        """
        带重试的文件下载

        Args:
            url: 下载URL
            local_path: 本地保存路径
            headers: 请求头

        Returns:
            bool: 是否成功
            str: 消息或错误
        """
        for attempt in range(self.retry_count):
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    stream=True,
                    timeout=self.timeout
                )

                if response.status_code != 200:
                    logger.warning(f"下载失败 (HTTP {response.status_code}), 尝试 {attempt + 1}/{self.retry_count}")
                    time.sleep(self.delay_between_retry)
                    continue

                # 获取文件大小
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0

                # 将内容保存到临时文件
                with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:  # 过滤空数据块
                            temp_file.write(chunk)
                            downloaded += len(chunk)

                            # 打印下载进度
                            if total_size > 0:
                                progress = downloaded / total_size * 100
                                if downloaded % (1024 * 1024) == 0:  # 每下载1MB打印一次
                                    logger.debug(
                                        f"下载进度: {progress:.1f}% ({downloaded / 1024 / 1024:.1f}MB/{total_size / 1024 / 1024:.1f}MB)")

                # 下载完成后移动临时文件到目标位置
                shutil.move(temp_file.name, local_path)
                logger.info(f"下载完成: {os.path.basename(local_path)} ({downloaded / 1024 / 1024:.1f}MB)")
                return True, "下载成功"

            except requests.RequestException as e:
                logger.warning(f"下载失败: {e}, 尝试 {attempt + 1}/{self.retry_count}")
                if attempt < self.retry_count - 1:
                    time.sleep(self.delay_between_retry)

        return False, f"下载失败，已重试{self.retry_count}次"

    def _download_direct_file(self, url, dataset_dir, name=None):
        """
        下载直接文件链接

        Args:
            url: 文件URL
            dataset_dir: 保存目录
            name: 文件名

        Returns:
            bool: 是否成功
            str: 消息或错误
        """
        try:
            # 获取文件名
            if name:
                # 从URL中提取文件扩展名
                parsed_url = urlparse(url)
                path = unquote(parsed_url.path)
                _, ext = os.path.splitext(path)

                # 如果name没有扩展名，使用URL中的扩展名
                if not os.path.splitext(name)[1] and ext:
                    filename = f"{name}{ext}"
                else:
                    filename = name
            else:
                # 从URL提取文件名
                parsed_url = urlparse(url)
                path = unquote(parsed_url.path)
                filename = os.path.basename(path)

                # 如果URL路径未提供有效文件名
                if not filename or '.' not in filename:
                    filename = f"file_{int(time.time())}.bin"

            # 确保文件名有效
            filename = self._sanitize_filename(filename)

            # 完整保存路径
            local_path = os.path.join(dataset_dir, filename)

            # 下载文件
            logger.info(f"直接下载文件: {url} -> {local_path}")
            return self._download_with_retry(url, local_path)

        except Exception as e:
            logger.error(f"下载文件失败: {e}")
            return False, f"下载文件失败: {str(e)}"

    def _download_webpage(self, url, dataset_dir):
        """
        处理普通网页

        Args:
            url: 网页URL
            dataset_dir: 保存目录

        Returns:
            bool: 是否成功
            str: 消息或错误
        """
        try:
            # 保存页面HTML
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            path = parsed_url.path.rstrip('/')

            # 创建HTML文件名
            if path:
                last_part = os.path.basename(path)
                html_filename = f"{domain}_{last_part}.html"
            else:
                html_filename = f"{domain}_index.html"

            html_filename = self._sanitize_filename(html_filename)
            html_path = os.path.join(dataset_dir, html_filename)

            # 下载HTML
            logger.info(f"下载网页: {url} -> {html_path}")
            success, message = self._download_html(url, html_path)

            if not success:
                return False, message

            # 创建元数据文件
            meta_filename = "metadata.json"
            meta_path = os.path.join(dataset_dir, meta_filename)

            metadata = {
                "url": url,
                "downloaded_at": time.strftime('%Y-%m-%d %H:%M:%S'),
                "html_file": html_filename
            }

            with open(meta_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)

            return True, "成功下载网页和元数据"

        except Exception as e:
            logger.error(f"下载网页失败: {e}")
            return False, f"下载网页失败: {str(e)}"

    def _download_html(self, url, local_path):
        """
        下载网页HTML

        Args:
            url: 网页URL
            local_path: 保存路径

        Returns:
            bool: 是否成功
            str: 消息或错误
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5'
            }

            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code != 200:
                return False, f"HTTP错误: {response.status_code}"

            # 确定编码
            if response.encoding is None:
                response.encoding = 'utf-8'

            # 保存HTML内容
            with open(local_path, 'w', encoding=response.encoding, errors='replace') as f:
                f.write(response.text)

            return True, "成功下载HTML"

        except Exception as e:
            logger.error(f"下载HTML失败: {e}")
            return False, f"下载HTML失败: {str(e)}"

    def _download_github(self, url, dataset_dir):
        """
        下载GitHub仓库

        Args:
            url: GitHub URL
            dataset_dir: 保存目录

        Returns:
            bool: 是否成功
            str: 消息或错误
        """
        # GitHub下载逻辑
        try:
            # 尝试将GitHub页面URL转换为下载ZIP的URL
            github_pattern = r'https?://github\.com/([^/]+)/([^/]+)'
            match = re.match(github_pattern, url)

            if not match:
                return self._download_webpage(url, dataset_dir)

            owner, repo = match.groups()

            # 去除repo名称中可能的.git后缀
            repo = repo.replace('.git', '')

            # GitHub仓库ZIP下载链接
            zip_url = f"https://github.com/{owner}/{repo}/archive/refs/heads/main.zip"

            # 尝试下载main分支，如果失败则尝试master分支
            local_path = os.path.join(dataset_dir, f"{owner}_{repo}.zip")

            logger.info(f"尝试下载GitHub仓库: {zip_url}")
            success, message = self._download_with_retry(zip_url, local_path)

            if not success:
                # 尝试master分支
                zip_url = f"https://github.com/{owner}/{repo}/archive/refs/heads/master.zip"
                logger.info(f"尝试下载GitHub仓库: {zip_url}")
                success, message = self._download_with_retry(zip_url, local_path)

            if success:
                return True, "成功下载GitHub仓库"
            else:
                # 如果无法下载ZIP，则保存页面
                return self._download_webpage(url, dataset_dir)

        except Exception as e:
            logger.error(f"下载GitHub仓库失败: {e}")
            return False, f"下载GitHub仓库失败: {str(e)}"

    def _download_figshare(self, url, dataset_dir):
        """
        从Figshare下载数据集

        Args:
            url: Figshare URL
            dataset_dir: 保存目录

        Returns:
            bool: 是否成功
            str: 消息或错误
        """
        try:
            logger.info(f"开始处理Figshare数据集: {url}")

            # 首先下载HTML页面
            html_path = os.path.join(dataset_dir, "figshare_page.html")

            # 使用更完整的headers，部分网站会检查User-Agent
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Referer': 'https://figshare.com/'
            }

            response = requests.get(url, headers=headers, timeout=30)

            if response.status_code != 200:
                logger.error(f"获取Figshare页面失败: HTTP {response.status_code}")
                return self._download_webpage(url, dataset_dir)  # 退回到普通网页下载

            # 保存HTML
            with open(html_path, 'w', encoding='utf-8', errors='replace') as f:
                f.write(response.text)

            # 解析HTML寻找下载链接
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')

            # 查找下载链接
            download_links = []

            # 1. 寻找直接下载链接 (href包含download或.zip/.tar等扩展名)
            for a in soup.find_all('a', href=True):
                href = a.get('href', '')
                text = a.get_text().lower()

                # 如果链接文本或类名包含"download"，或者href指向下载文件
                if ('download' in text or
                    'download' in a.get('class', [''])[0].lower() if a.get('class') else False or
                                                                                         any(href.endswith(ext) for ext
                                                                                             in ['.zip', '.tar', '.gz',
                                                                                                 '.csv', '.nii.gz',
                                                                                                 '.mat'])):

                    # 确保是绝对URL
                    if href.startswith('http'):
                        download_links.append(href)
                    else:
                        # 相对URL转为绝对URL
                        download_links.append(urljoin(url, href))

            # 2. 查找figshare特定的下载元素
            figshare_buttons = soup.select('[data-test="download"] a, [data-url-type="download"] a, .downloads a')
            for button in figshare_buttons:
                href = button.get('href')
                if href:
                    if href.startswith('http'):
                        download_links.append(href)
                    else:
                        download_links.append(urljoin(url, href))

            # 3. 检查JSON数据中的下载链接
            scripts = soup.find_all('script', type='application/ld+json')
            for script in scripts:
                try:
                    data = json.loads(script.string)
                    if isinstance(data, dict) and 'contentUrl' in data:
                        download_links.append(data['contentUrl'])
                    elif isinstance(data, dict) and 'distribution' in data:
                        if isinstance(data['distribution'], list):
                            for dist in data['distribution']:
                                if 'contentUrl' in dist:
                                    download_links.append(dist['contentUrl'])
                        elif isinstance(data['distribution'], dict) and 'contentUrl' in data['distribution']:
                            download_links.append(data['distribution']['contentUrl'])
                except (json.JSONDecodeError, TypeError):
                    pass

            # 移除重复链接并过滤无效链接
            download_links = list(set([link for link in download_links if link and 'http' in link]))

            if download_links:
                logger.info(f"在Figshare页面找到 {len(download_links)} 个下载链接")

                # 创建元数据文件，记录所有发现的链接
                meta_path = os.path.join(dataset_dir, "download_links.json")
                with open(meta_path, 'w', encoding='utf-8') as f:
                    json.dump({
                        'source_url': url,
                        'download_links': download_links,
                        'extracted_at': datetime.now().isoformat()
                    }, f, indent=2)

                # 下载第一个文件
                success_count = 0
                for i, download_url in enumerate(download_links[:5]):  # 最多下载前5个文件
                    try:
                        # 从URL中提取文件名
                        filename = os.path.basename(urlparse(download_url).path)

                        # 如果文件名为空或无效，使用索引生成文件名
                        if not filename or '.' not in filename:
                            filename = f"figshare_file_{i + 1}.bin"

                        # 清理文件名
                        filename = self._sanitize_filename(filename)

                        # 完整的保存路径
                        file_path = os.path.join(dataset_dir, filename)

                        logger.info(f"尝试下载文件 {i + 1}/{len(download_links)}: {download_url}")
                        success, message = self._download_with_retry(download_url, file_path, headers=headers)

                        if success:
                            success_count += 1
                        else:
                            logger.warning(f"下载链接 {download_url} 失败: {message}")

                    except Exception as e:
                        logger.error(f"处理下载链接时出错: {e}")

                if success_count > 0:
                    return True, f"成功下载 {success_count} 个Figshare文件"
                else:
                    # 如果所有文件下载都失败，退回到网页下载
                    logger.warning("所有Figshare链接下载失败，保存为网页")
                    return self._download_webpage(url, dataset_dir)
            else:
                logger.warning(f"在Figshare页面未找到下载链接，保存为网页")
                return self._download_webpage(url, dataset_dir)

        except Exception as e:
            logger.error(f"处理Figshare数据集时出错: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # 出错时退回到普通网页下载
            return self._download_webpage(url, dataset_dir)

    def _download_zenodo(self, url, dataset_dir):
        """下载Zenodo数据集"""
        # 目前简单保存网页，后续可以使用Zenodo API
        return self._download_webpage(url, dataset_dir)

    def _download_osf(self, url, dataset_dir):
        """下载OSF数据集"""
        # 目前简单保存网页，后续可以使用OSF API
        return self._download_webpage(url, dataset_dir)

    def _download_dryad(self, url, dataset_dir):
        """下载Dryad数据集"""
        # 目前简单保存网页，后续可以使用Dryad API
        return self._download_webpage(url, dataset_dir)

    def _download_dataverse(self, url, dataset_dir):
        """下载Dataverse数据集"""
        # 目前简单保存网页，后续可以实现Dataverse API
        return self._download_webpage(url, dataset_dir)

    def _download_kaggle(self, url, dataset_dir):
        """下载Kaggle数据集"""
        # 目前简单保存网页，后续可以使用Kaggle API
        return self._download_webpage(url, dataset_dir)

    def _sanitize_filename(self, filename):
        """
        清理文件名

        Args:
            filename: 原始文件名

        Returns:
            str: 安全的文件名
        """
        # 转换为字符串
        filename = str(filename)

        # 移除非法字符
        illegal_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
        for char in illegal_chars:
            filename = filename.replace(char, '_')

        # 限制长度
        max_length = 100
        if len(filename) > max_length:
            base, ext = os.path.splitext(filename)
            if len(ext) > 10:  # 如果扩展名异常长
                ext = ext[:10]
            base = base[:max_length - len(ext)]
            filename = base + ext

        return filename