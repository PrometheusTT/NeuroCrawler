# 向现有的utils/data_utils.py添加以下内容（如果文件不存在则创建）

import os
import hashlib
import logging
import requests
import time
import re
from urllib.parse import urlparse, unquote
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
import concurrent.futures

logger = logging.getLogger(__name__)


class DatasetDownloader:
    """数据集下载器，支持多种常见数据仓库"""

    def __init__(self, config=None):
        """
        初始化数据集下载器

        Args:
            config: 下载相关配置
        """
        self.config = config or {}
        self.base_dir = self.config.get('download_dir', 'datasets')
        self.max_retries = self.config.get('max_retries', 3)
        self.timeout = self.config.get('timeout', 60)
        self.chunk_size = self.config.get('chunk_size', 8192)
        self.max_workers = self.config.get('max_workers', 3)

        # 确保下载目录存在
        os.makedirs(self.base_dir, exist_ok=True)

        # 下载历史文件
        self.history_file = os.path.join(self.base_dir, 'download_history.json')
        self.download_history = self._load_history()

        # API密钥
        self.api_keys = self.config.get('api_keys', {})

    def download_dataset(self, dataset_info):
        """
        下载单个数据集

        Args:
            dataset_info: 包含数据集信息的字典

        Returns:
            dict: 下载结果信息
        """
        # 提取关键信息
        dataset_id = dataset_info.get('id') or dataset_info.get('doi') or dataset_info.get('url')
        dataset_name = dataset_info.get('name', '未命名数据集')
        url = dataset_info.get('url')
        repository = dataset_info.get('repository', self._detect_repository(url))

        result = {
            'dataset': dataset_name,
            'url': url,
            'repository': repository,
            'timestamp': datetime.now().isoformat(),
            'success': False
        }

        # 检查是否已下载
        if self._is_downloaded(dataset_id):
            logger.info(f"数据集 '{dataset_name}' 已下载，跳过")
            result['status'] = 'skipped'
            return result

        if not url:
            logger.warning(f"数据集 '{dataset_name}' 没有下载URL")
            result['error'] = 'missing_url'
            return result

        # 创建数据集目录
        dataset_dir = self._create_dataset_dir(dataset_info)

        try:
            # 根据仓库类型选择下载方法
            if repository == 'figshare':
                download_info = self._download_from_figshare(dataset_info, dataset_dir)
            elif repository == 'zenodo':
                download_info = self._download_from_zenodo(dataset_info, dataset_dir)
            elif repository == 'dryad':
                download_info = self._download_from_dryad(dataset_info, dataset_dir)
            elif repository == 'osf':
                download_info = self._download_from_osf(dataset_info, dataset_dir)
            elif repository == 'github':
                download_info = self._download_from_github(dataset_info, dataset_dir)
            else:
                # 直接下载
                download_info = self._download_direct(dataset_info, dataset_dir)

            if download_info.get('success'):
                # 更新下载历史
                self._update_history(dataset_id, dataset_info, download_info)

                result.update({
                    'success': True,
                    'files': download_info.get('files', []),
                    'total_size': download_info.get('total_size', 0),
                    'path': dataset_dir
                })
                logger.info(f"成功下载数据集 '{dataset_name}' 到 {dataset_dir}")
            else:
                result['error'] = download_info.get('error', '下载失败')
                logger.error(f"下载数据集 '{dataset_name}' 失败: {result['error']}")

        except Exception as e:
            result['error'] = str(e)
            logger.exception(f"下载数据集 '{dataset_name}' 时出错:")

        return result

    def download_datasets(self, datasets):
        """
        批量下载多个数据集

        Args:
            datasets: 数据集信息列表

        Returns:
            dict: 下载结果统计
        """
        if not datasets:
            logger.info("没有数据集需要下载")
            return {"success": 0, "failed": 0, "skipped": 0, "total": 0}

        logger.info(f"开始下载 {len(datasets)} 个数据集")

        results = {
            "total": len(datasets),
            "success": 0,
            "failed": 0,
            "skipped": 0,
            "details": []
        }

        # 使用线程池并行下载
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_dataset = {
                executor.submit(self.download_dataset, dataset): dataset
                for dataset in datasets
            }

            for future in concurrent.futures.as_completed(future_to_dataset):
                dataset = future_to_dataset[future]
                try:
                    result = future.result()
                    results["details"].append(result)

                    if result.get('success'):
                        results["success"] += 1
                    elif result.get('status') == 'skipped':
                        results["skipped"] += 1
                    else:
                        results["failed"] += 1
                except Exception as e:
                    logger.error(f"处理数据集下载时出错: {e}")
                    results["failed"] += 1

        # 保存下载历史
        self._save_history()

        logger.info(f"数据集下载完成: 成功 {results['success']}, 失败 {results['failed']}, "
                    f"跳过 {results['skipped']}, 总计 {results['total']}")

        return results

    def _download_file(self, url, filepath, headers=None):
        """下载单个文件，支持重试和进度条"""
        if headers is None:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/90.0.4430.212 Safari/537.36'
            }

        result = {"success": False}

        for attempt in range(self.max_retries):
            try:
                with requests.get(url, headers=headers, stream=True, timeout=self.timeout) as response:
                    if response.status_code != 200:
                        logger.warning(
                            f"下载失败 (尝试 {attempt + 1}/{self.max_retries}), 状态码: {response.status_code}, URL: {url}")
                        time.sleep(2)
                        continue

                    # 获取文件大小
                    total_size = int(response.headers.get('content-length', 0))
                    result["size"] = total_size

                    # 计算文件哈希值
                    file_hash = hashlib.md5()

                    # 使用tqdm显示进度条
                    with open(filepath, 'wb') as f, tqdm(
                            desc=os.path.basename(filepath),
                            total=total_size,
                            unit='B',
                            unit_scale=True,
                            unit_divisor=1024,
                    ) as bar:
                        for chunk in response.iter_content(chunk_size=self.chunk_size):
                            if chunk:
                                f.write(chunk)
                                bar.update(len(chunk))
                                file_hash.update(chunk)

                    # 成功完成下载
                    result.update({
                        "success": True,
                        "file_path": filepath,
                        "file_name": os.path.basename(filepath),
                        "file_hash": file_hash.hexdigest()
                    })

                    return result

            except requests.RequestException as e:
                logger.warning(f"下载尝试 {attempt + 1}/{self.max_retries} 失败: {e}")
                time.sleep(2)

        result["error"] = f"多次尝试后下载失败: {url}"
        return result

    def _create_dataset_dir(self, dataset_info):
        """为数据集创建目录"""
        # 从数据集名称创建安全的目录名
        dataset_name = dataset_info.get('name', '未命名数据集')
        paper_title = dataset_info.get('paper_title', '')
        date_str = datetime.now().strftime("%Y%m%d")

        # 创建安全的目录名
        safe_name = re.sub(r'[^\w\s-]', '', dataset_name).strip()
        safe_name = re.sub(r'[-\s]+', '_', safe_name)

        # 使用数据集ID或DOI作为唯一标识符
        dataset_id = dataset_info.get('id') or dataset_info.get('doi', '')
        if dataset_id:
            # 从DOI或ID创建安全标识符
            safe_id = re.sub(r'[^\w\.-]', '_', dataset_id)
        else:
            # 如果没有ID，使用名称的哈希值
            safe_id = hashlib.md5(dataset_name.encode('utf-8')).hexdigest()[:8]

        # 构建数据集目录路径
        folder_name = f"{date_str}_{safe_name}_{safe_id}"
        dataset_dir = os.path.join(self.base_dir, folder_name)

        # 创建目录
        os.makedirs(dataset_dir, exist_ok=True)

        return dataset_dir

    def _detect_repository(self, url):
        """根据URL识别数据仓库类型"""
        if not url:
            return "unknown"

        url_lower = url.lower()

        # 识别常见数据仓库
        if 'figshare.com' in url_lower:
            return 'figshare'
        elif 'zenodo.org' in url_lower:
            return 'zenodo'
        elif 'datadryad.org' in url_lower or 'dryad' in url_lower:
            return 'dryad'
        elif 'osf.io' in url_lower:
            return 'osf'
        elif 'github.com' in url_lower:
            return 'github'
        elif 'gin.g-node.org' in url_lower:
            return 'gin'
        elif 'dataverse' in url_lower:
            return 'dataverse'

        return 'direct'

    def _is_downloaded(self, dataset_id):
        """检查数据集是否已下载"""
        if not dataset_id:
            return False
        return dataset_id in self.download_history

    def _load_history(self):
        """加载下载历史"""
        import json
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"加载下载历史失败: {e}")
        return {}

    def _save_history(self):
        """保存下载历史"""
        import json
        try:
            with open(self.history_file, 'w', encoding='utf-8') as f:
                json.dump(self.download_history, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存下载历史失败: {e}")

    def _update_history(self, dataset_id, dataset_info, download_info):
        """更新下载历史"""
        if not dataset_id:
            return

        self.download_history[dataset_id] = {
            "name": dataset_info.get("name", "未命名数据集"),
            "paper_title": dataset_info.get("paper_title", ""),
            "paper_url": dataset_info.get("paper_url", ""),
            "doi": dataset_info.get("doi", ""),
            "repository": dataset_info.get("repository", "unknown"),
            "url": dataset_info.get("url", ""),
            "download_date": datetime.now().isoformat(),
            "download_path": download_info.get("path", ""),
            "files": download_info.get("files", []),
            "total_size": download_info.get("total_size", 0)
        }

    # 以下是各种数据仓库的具体下载实现
    def _download_direct(self, dataset_info, output_dir):
        """直接从URL下载文件"""
        url = dataset_info.get('url')
        result = {"success": False, "files": [], "total_size": 0}

        try:
            # 从URL获取文件名
            parsed_url = urlparse(url)
            file_name = os.path.basename(unquote(parsed_url.path))

            # 如果URL没有有效的文件名，创建一个
            if not file_name or len(file_name) < 3:
                file_name = f"dataset_{hashlib.md5(url.encode()).hexdigest()[:8]}"

                # 尝试从内容类型添加扩展名
                content_type = dataset_info.get('content_type', '')
                if 'zip' in content_type or '.zip' in url:
                    file_name += '.zip'
                elif 'csv' in content_type or '.csv' in url:
                    file_name += '.csv'
                elif 'excel' in content_type or '.xls' in url:
                    file_name += '.xlsx'
                elif 'pdf' in content_type or '.pdf' in url:
                    file_name += '.pdf'

            # 下载路径
            output_path = os.path.join(output_dir, file_name)

            # 设置请求头
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36'}

            # 对某些网站设置Referer
            parsed_url = urlparse(url)
            headers['Referer'] = f"{parsed_url.scheme}://{parsed_url.netloc}/"

            # 下载文件
            file_result = self._download_file(url, output_path, headers=headers)

            if file_result.get("success"):
                result["files"].append({
                    "name": file_name,
                    "path": output_path,
                    "size": file_result.get("size", 0),
                    "hash": file_result.get("file_hash")
                })
                result["total_size"] += file_result.get("size", 0)
                result["success"] = True
            else:
                result["error"] = file_result.get("error", "文件下载失败")

            return result

        except Exception as e:
            result["error"] = f"下载错误: {str(e)}"
            return result

    def _download_from_figshare(self, dataset_info, output_dir):
        """从Figshare下载数据集"""
        url = dataset_info.get('url')
        result = {"success": False, "files": [], "total_size": 0}

        try:
            # 解析Figshare URL获取文章ID
            match = re.search(r'figshare\.com/articles/(?:dataset/)?(\d+)', url)
            if not match:
                result["error"] = "无法从URL解析Figshare文章ID"
                return result

            article_id = match.group(1)

            # 使用Figshare API获取文件信息
            api_url = f"https://api.figshare.com/v2/articles/{article_id}"
            response = requests.get(api_url, timeout=self.timeout)

            if response.status_code != 200:
                result["error"] = f"Figshare API错误: {response.status_code}"
                return result

            article_data = response.json()

            # 下载所有文件
            for file_info in article_data.get("files", []):
                file_name = file_info.get("name")
                download_url = file_info.get("download_url")

                if not file_name or not download_url:
                    continue

                output_path = os.path.join(output_dir, file_name)

                # 下载文件
                file_result = self._download_file(download_url, output_path)

                if file_result.get("success"):
                    result["files"].append({
                        "name": file_name,
                        "path": output_path,
                        "size": file_result.get("size", 0),
                        "hash": file_result.get("file_hash")
                    })
                    result["total_size"] += file_result.get("size", 0)
                else:
                    result["error"] = file_result.get("error", "文件下载失败")
                    return result

            # 如果成功下载了所有文件
            if result["files"]:
                result["success"] = True
            else:
                result["error"] = "没有找到可下载的文件"

            return result

        except Exception as e:
            result["error"] = f"Figshare下载错误: {str(e)}"
            return result

    def _download_from_zenodo(self, dataset_info, output_dir):
        """从Zenodo下载数据集"""
        url = dataset_info.get('url')
        result = {"success": False, "files": [], "total_size": 0}

        try:
            # 解析Zenodo URL
            match = re.search(r'zenodo\.org/record/(\d+)', url)
            if not match:
                result["error"] = "无法从URL解析Zenodo记录ID"
                return result

            record_id = match.group(1)

            # 使用Zenodo API
            api_url = f"https://zenodo.org/api/records/{record_id}"
            response = requests.get(api_url, timeout=self.timeout)

            if response.status_code != 200:
                result["error"] = f"Zenodo API错误: {response.status_code}"
                return result

            record_data = response.json()

            # 下载文件
            for file_info in record_data.get("files", []):
                file_name = file_info.get("key")
                download_url = file_info.get("links", {}).get("self")

                if not file_name or not download_url:
                    continue

                output_path = os.path.join(output_dir, file_name)

                # 下载文件
                file_result = self._download_file(download_url, output_path)

                if file_result.get("success"):
                    result["files"].append({
                        "name": file_name,
                        "path": output_path,
                        "size": file_result.get("size", 0),
                        "hash": file_result.get("file_hash")
                    })
                    result["total_size"] += file_result.get("size", 0)
                else:
                    result["error"] = file_result.get("error", "文件下载失败")
                    return result

            # 如果成功下载了所有文件
            if result["files"]:
                result["success"] = True
            else:
                result["error"] = "没有找到可下载的文件"

            return result

        except Exception as e:
            result["error"] = f"Zenodo下载错误: {str(e)}"
            return result

    def _download_from_dryad(self, dataset_info, output_dir):
        """从Dryad下载数据集"""
        url = dataset_info.get('url')
        result = {"success": False, "files": [], "total_size": 0}

        # Dryad的实现与direct下载类似，但需要处理特定的URL格式
        try:
            # 通常Dryad数据集会提供直接下载链接
            # 尝试找到下载链接
            response = requests.get(url, timeout=self.timeout)

            if response.status_code != 200:
                result["error"] = f"无法访问Dryad页面: {response.status_code}"
                return result

            # 使用正则表达式查找下载链接
            download_links = re.findall(r'href="(https://datadryad\.org/stash/downloads/file_stream/\d+)"',
                                        response.text)

            if not download_links:
                # 尝试其他格式
                download_links = re.findall(r'href="(/stash/downloads/[^"]+)"', response.text)
                download_links = [f"https://datadryad.org{link}" if not link.startswith('http') else link for link in
                                  download_links]

            if not download_links:
                result["error"] = "在Dryad页面上未找到下载链接"
                return result

            # 下载找到的文件
            for link in download_links:
                # 从URL获取文件名
                file_name = os.path.basename(urlparse(link).path)

                # 创建有意义的文件名（如果URL中没有）
                if not file_name or len(file_name) < 3:
                    file_name = f"dryad_file_{len(result['files']) + 1}.zip"

                output_path = os.path.join(output_dir, file_name)

                # 下载文件
                file_result = self._download_file(link, output_path)

                if file_result.get("success"):
                    result["files"].append({
                        "name": file_name,
                        "path": output_path,
                        "size": file_result.get("size", 0),
                        "hash": file_result.get("file_hash")
                    })
                    result["total_size"] += file_result.get("size", 0)

            # 如果成功下载了文件
            if result["files"]:
                result["success"] = True
            else:
                result["error"] = "无法下载Dryad文件"

            return result

        except Exception as e:
            result["error"] = f"Dryad下载错误: {str(e)}"
            return result

    def _download_from_github(self, dataset_info, output_dir):
        """从GitHub下载数据集"""
        url = dataset_info.get('url')
        result = {"success": False, "files": [], "total_size": 0}

        try:
            # 解析GitHub URL
            match = re.search(r'github\.com/([^/]+)/([^/]+)', url)
            if not match:
                result["error"] = "无法从URL解析GitHub仓库信息"
                return result

            username = match.group(1)
            repo = match.group(2)

            # 直接构建ZIP下载链接
            # 假设使用主分支
            download_url = f"https://github.com/{username}/{repo}/archive/refs/heads/master.zip"
            file_name = f"{repo}-master.zip"
            output_path = os.path.join(output_dir, file_name)

            # 设置请求头
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36'
            }

            # 如果有GitHub Token，使用它
            if "github" in self.api_keys:
                headers["Authorization"] = f"token {self.api_keys['github']}"

            # 下载ZIP文件
            file_result = self._download_file(download_url, output_path, headers=headers)

            if file_result.get("success"):
                result["files"].append({
                    "name": file_name,
                    "path": output_path,
                    "size": file_result.get("size", 0),
                    "hash": file_result.get("file_hash")
                })
                result["total_size"] += file_result.get("size", 0)
                result["success"] = True
            else:
                # 尝试main分支
                download_url = f"https://github.com/{username}/{repo}/archive/refs/heads/main.zip"
                file_name = f"{repo}-main.zip"
                output_path = os.path.join(output_dir, file_name)

                file_result = self._download_file(download_url, output_path, headers=headers)

                if file_result.get("success"):
                    result["files"].append({
                        "name": file_name,
                        "path": output_path,
                        "size": file_result.get("size", 0),
                        "hash": file_result.get("file_hash")
                    })
                    result["total_size"] += file_result.get("size", 0)
                    result["success"] = True
                else:
                    result["error"] = "GitHub仓库下载失败，尝试了master和main分支"

            return result

        except Exception as e:
            result["error"] = f"GitHub下载错误: {str(e)}"
            return result

    def _download_from_osf(self, dataset_info, output_dir):
        """从OSF下载数据集"""
        # OSF实现类似于其他仓库
        url = dataset_info.get('url')
        result = {"success": False, "files": [], "total_size": 0}

        try:
            # 解析OSF URL
            match = re.search(r'osf\.io/([a-zA-Z0-9]+)', url)
            if not match:
                result["error"] = "无法从URL解析OSF项目ID"
                return result

            project_id = match.group(1)

            # 尝试访问原始页面找到下载链接
            response = requests.get(url, timeout=self.timeout)

            if response.status_code != 200:
                result["error"] = f"无法访问OSF页面: {response.status_code}"
                return result

            # 查找下载链接
            download_links = re.findall(r'href="(https://osf\.io/[^/]+/download[^"]*)"', response.text)

            if not download_links:
                result["error"] = "在OSF页面上未找到下载链接"
                return result

            # 下载找到的文件
            for link in download_links:
                # 尝试从链接获取文件名
                parsed_url = urlparse(link)
                file_name = os.path.basename(parsed_url.path)

                # 如果没有有效文件名，创建一个
                if not file_name or file_name == "download":
                    file_name = f"osf_file_{len(result['files']) + 1}"

                output_path = os.path.join(output_dir, file_name)

                # 下载文件
                file_result = self._download_file(link, output_path)

                if file_result.get("success"):
                    result["files"].append({
                        "name": file_name,
                        "path": output_path,
                        "size": file_result.get("size", 0),
                        "hash": file_result.get("file_hash")
                    })
                    result["total_size"] += file_result.get("size", 0)

            # 如果成功下载了文件
            if result["files"]:
                result["success"] = True
            else:
                result["error"] = "无法下载OSF文件"

            return result

        except Exception as e:
            result["error"] = f"OSF下载错误: {str(e)}"
            return result