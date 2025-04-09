#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import time
import logging
import requests
from datetime import datetime, timedelta
from urllib.parse import urlparse, urljoin
from utils.data_utils import DataDownloader

logger = logging.getLogger(__name__)


class DatasetDownloadManager:
    """数据集下载管理器，负责协调数据集的获取与下载"""

    def __init__(self, config=None):
        """
        初始化下载管理器

        Args:
            config: 配置字典
        """
        self.config = config or {}
        self.downloader_config = self.config.get('downloader', {})

        # 设置下载目录
        if 'download_dir' not in self.downloader_config:
            self.downloader_config['download_dir'] = os.path.join(os.getcwd(), 'datasets')

        # 初始化下载器
        self.downloader = DataDownloader(self.downloader_config)

        # 最大并行下载数
        self.max_concurrent_downloads = self.downloader_config.get('max_concurrent', 3)

        # 数据集类型映射（用于过滤）
        self.data_type_mapping = {
            'neuron_imaging': ['neuron_imaging', 'neural_imaging', 'brain_imaging', 'microscopy'],
            'reconstruction': ['reconstruction', 'connectomics', 'morphology', 'connectivity'],
            'spatial_transcriptomics': ['spatial_transcriptomics', 'transcriptomics', 'gene_expression'],
            'mri': ['mri', 'fmri', 'magnetic_resonance', 'diffusion_imaging', 'structural_imaging'],
            'electrophysiology': ['electrophysiology', 'ephys', 'patch_clamp', 'eeg', 'ecog', 'spike_sorting']
        }

        logger.info(f"数据集下载管理器初始化完成，下载目录: {self.downloader_config['download_dir']}")

    def download_datasets_from_crawler_results(self, papers, max_datasets=None, data_types=None):
        """
        从爬虫结果中提取并下载数据集

        Args:
            papers: 爬虫爬取的论文列表
            max_datasets: 最大下载数量
            data_types: 数据类型过滤

        Returns:
            dict: 下载结果统计
        """
        if not papers:
            logger.info("没有提供论文，无法提取数据集")
            return {"success": 0, "failed": 0, "skipped": 0, "total": 0}

        # 从论文中提取数据集
        logger.info(f"从 {len(papers)} 篇论文中提取数据集")
        datasets = []

        for paper in papers:
            if 'datasets' in paper and paper['datasets']:
                # 添加论文信息到数据集
                for dataset in paper['datasets']:
                    dataset['paper_title'] = paper.get('title', '')
                    dataset['paper_url'] = paper.get('url', '')
                    dataset['paper_doi'] = paper.get('doi', '')
                datasets.extend(paper['datasets'])

        if not datasets:
            logger.info("未从论文中找到数据集")
            return {"success": 0, "failed": 0, "skipped": 0, "total": 0}

        # 过滤数据集
        if data_types:
            filtered_datasets = []
            expanded_types = []

            # 展开数据类型别名
            for data_type in data_types:
                if data_type in self.data_type_mapping:
                    expanded_types.extend(self.data_type_mapping[data_type])
                else:
                    expanded_types.append(data_type)

            # 过滤数据集
            for dataset in datasets:
                dataset_types = dataset.get('data_types', [])
                if not dataset_types:
                    # 如果数据集没有类型标记，尝试从名称或描述中推断
                    description = dataset.get('description', '').lower()
                    name = dataset.get('name', '').lower()
                    combined_text = description + ' ' + name

                    if any(data_type.lower() in combined_text for data_type in expanded_types):
                        filtered_datasets.append(dataset)
                elif any(data_type in dataset_types for data_type in expanded_types):
                    filtered_datasets.append(dataset)

            datasets = filtered_datasets
            logger.info(f"根据数据类型过滤后剩余 {len(datasets)} 个数据集")

        # 限制数量
        if max_datasets and len(datasets) > max_datasets:
            datasets = datasets[:max_datasets]
            logger.info(f"限制下载数量为前 {max_datasets} 个数据集")

        # 下载数据集
        return self.downloader.download_datasets(datasets)

    def download_datasets_from_database(self, days=None, start_date=None, end_date=None,
                                        max_datasets=None, data_types=None, sources=None):
        """从数据库中下载数据集

        Args:
            days: 过去几天的数据
            start_date: 开始日期
            end_date: 结束日期
            max_datasets: 最大下载数量
            data_types: 数据类型过滤列表
            sources: 来源过滤列表

        Returns:
            dict: 下载结果统计
        """
        from database.operations import get_datasets_by_criteria

        logger.info(f"从数据库查询符合条件的数据集...")
        logger.info(f"过滤条件: 开始日期={start_date}, 结束日期={end_date}, 数据类型={data_types}, 来源={sources}")

        filters = {}

        # 构建日期过滤条件
        if days:
            filters['days'] = days
        elif start_date and end_date:
            filters['start_date'] = start_date
            filters['end_date'] = end_date

        # 其他过滤条件
        if data_types:
            filters['data_types'] = data_types
        if sources:
            filters['sources'] = sources

        # 查询数据库
        datasets = get_datasets_by_criteria(**filters, limit=max_datasets)

        if datasets:
            logger.info(f"从数据库中获取到 {len(datasets)} 个符合条件的数据集")

            # 显示将要下载的数据集信息
            for i, dataset in enumerate(datasets[:min(5, len(datasets))]):
                try:
                    data_types_str = ""
                    if dataset.get('data_types'):
                        if isinstance(dataset['data_types'], list):
                            data_types_str = ", ".join(dataset['data_types'])
                        elif isinstance(dataset['data_types'], str):
                            data_types_str = dataset['data_types']

                    logger.info(
                        f"数据集 {i + 1}: {dataset.get('name')} ({data_types_str}) - {dataset.get('repository', 'unknown')}")
                except Exception as e:
                    logger.error(f"显示数据集信息时出错: {e}")
                    logger.info(f"数据集 {i + 1}: {dataset.get('name', 'Unknown')}")

            if len(datasets) > 5:
                logger.info(f"... 以及其他 {len(datasets) - 5} 个数据集")

            return self.downloader.download_datasets(datasets)
        else:
            logger.info("数据库中没有找到符合条件的数据集")
            return {"success": 0, "failed": 0, "skipped": 0, "total": 0}

    def download_single_dataset(self, dataset_url, name=None, repository=None, force=False):
        """
        下载单个数据集

        Args:
            dataset_url: 数据集URL
            name: 数据集名称
            repository: 数据集来源平台
            force: 是否强制下载（忽略缓存）

        Returns:
            bool: 是否成功
            str: 消息或错误
        """
        # 自动检测数据集平台
        if not repository or repository == "自动检测":
            repository = self._detect_repository(dataset_url)

        # 如果没有指定名称，生成一个基于URL的名称
        if not name:
            name = self._generate_dataset_name(dataset_url)

        # 构建数据集信息字典
        dataset = {
            'url': dataset_url,
            'name': name,
            'repository': repository,
            'data_types': []
        }

        logger.info(f"开始下载数据集: {name} 从 {dataset_url} (来源: {repository})")

        # 检查是否已下载过
        if not force and self.downloader.is_dataset_downloaded(dataset):
            # 如果已下载并且不是强制模式，则清除缓存以重新下载
            download_id = self.downloader._get_download_id(dataset)
            if download_id in self.downloader.download_history:
                del self.downloader.download_history[download_id]
                self.downloader._save_download_history()
                logger.info(f"已清除缓存记录，将重新下载")

        # 创建下载目录
        dataset_dir = os.path.join(self.downloader_config.get('download_dir', 'datasets'),
                                   self._sanitize_filename(name))
        os.makedirs(dataset_dir, exist_ok=True)

        # 根据仓库类型选择下载方法
        special_downloaders = {
            'figshare': self.downloader._download_figshare,
            'zenodo': self.downloader._download_zenodo if hasattr(self.downloader, '_download_zenodo') else None,
            'osf': self.downloader._download_osf if hasattr(self.downloader, '_download_osf') else None,
            'dryad': self.downloader._download_dryad if hasattr(self.downloader, '_download_dryad') else None
        }

        # 检查是否有特殊处理程序
        if repository.lower() in special_downloaders and special_downloaders[repository.lower()]:
            logger.info(f"使用特殊处理程序下载 {repository} 数据集")
            try:
                # 直接调用特殊下载程序
                special_downloader = special_downloaders[repository.lower()]
                success, message = special_downloader(dataset_url, dataset_dir)

                if success:
                    logger.info(f"特殊处理程序下载成功: {message}")
                    # 更新下载历史
                    self.downloader._add_to_download_history(dataset)
                    return True, message
                else:
                    logger.warning(f"特殊处理程序下载失败: {message}，尝试常规方法")
            except Exception as e:
                logger.error(f"特殊处理程序出错: {e}")

        # 如果没有特殊处理程序或者特殊处理失败，使用常规下载器
        try:
            success, message = self.downloader.download_dataset(dataset)

            if success:
                if message == "已下载":
                    logger.info(f"数据集已存在于下载历史但被重新下载")
                else:
                    logger.info(f"使用常规下载器成功: {message}")
                return True, message
            else:
                logger.error(f"常规下载器失败: {message}")

                # 如果常规下载失败，尝试使用Selenium下载
                if self.downloader_config.get('use_selenium', False) and hasattr(self.downloader,
                                                                                 '_download_with_selenium'):
                    logger.info(f"尝试使用Selenium下载...")
                    try:
                        selenium_success, selenium_message = self.downloader._download_with_selenium(dataset_url,
                                                                                                     dataset_dir)
                        if selenium_success:
                            logger.info(f"Selenium下载成功: {selenium_message}")
                            # 更新下载历史
                            self.downloader._add_to_download_history(dataset)
                            return True, selenium_message
                        else:
                            logger.warning(f"Selenium下载失败: {selenium_message}")
                    except Exception as e:
                        logger.error(f"Selenium下载出错: {e}")

                return False, message

        except Exception as e:
            error_message = f"下载出错: {str(e)}"
            logger.error(error_message)
            return False, error_message

    def _detect_repository(self, url):
        """检测数据集来源平台"""
        url_lower = url.lower()

        mappings = [
            ("figshare.com", "figshare"),
            ("10.6084/m9.figshare", "figshare"),
            ("zenodo.org", "zenodo"),
            ("osf.io", "osf"),
            ("dataverse", "dataverse"),
            ("datadryad.org", "dryad"),
            ("openneuro.org", "openneuro"),
            ("crcns.org", "crcns"),
            ("ebrains.eu", "ebrains"),
            ("dandiarchive.org", "dandi"),
            ("nature.com", "nature"),
            ("science.org", "science"),
            ("cell.com", "cell"),
            ("github.com", "github"),
            ("huggingface.co", "huggingface"),
            ("kaggle.com", "kaggle"),
            ("ncbi.nlm.nih.gov", "ncbi"),
            ("drive.google.com", "google_drive"),
        ]

        for domain, repo in mappings:
            if domain in url_lower:
                return repo

        return "website"

    def _generate_dataset_name(self, url):
        """根据URL生成数据集名称"""
        try:
            parsed = urlparse(url)
            path = parsed.path.strip('/')

            # 如果路径为空，使用域名
            if not path:
                return parsed.netloc.replace('.', '_')

            # 使用路径最后一部分作为名称
            parts = path.split('/')
            name = parts[-1]

            # 清理名称
            name = re.sub(r'[^\w\-\.]', '_', name)

            # 如果名称为空或太短，使用更多路径部分
            if len(name) < 5 and len(parts) > 1:
                name = f"{parts[-2]}_{name}"

            # 添加时间戳确保唯一性
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            return f"{name}_{timestamp}"

        except:
            # 安全回退
            return f"dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    def _download_with_specialized_downloader(self, url, repository, name):
        """使用专门的下载器下载数据集"""
        try:
            # 动态导入专门的下载器 - 如果没有就使用Selenium
            try:
                if repository.lower() == 'figshare':
                    try:
                        from figshare_downloader import FigshareDownloader
                        downloader = FigshareDownloader(self.downloader_config.get('download_dir'))
                        return downloader.download(url)
                    except ImportError:
                        logger.warning("未找到专门的Figshare下载器，使用内部实现")
                        return self._download_figshare(url, name)
                else:
                    # 使用通用Selenium下载器
                    return self._download_with_selenium(url, name)
            except ImportError:
                logger.warning(f"未找到专门的{repository}下载器，尝试使用Selenium")
                return self._download_with_selenium(url, name)

        except Exception as e:
            logger.error(f"专门下载器错误: {e}")
            return False, f"专门下载器错误: {str(e)}"

    def _download_figshare(self, url, name):
        """内置的Figshare下载方法"""
        try:
            # 提取Figshare ID
            figshare_id = None

            # 尝试不同的URL模式
            if '10.6084/m9.figshare.' in url:
                # DOI模式: https://doi.org/10.6084/m9.figshare.12345678
                match = re.search(r'10\.6084/m9\.figshare\.(\d+)', url)
                if match:
                    figshare_id = match.group(1)
            elif 'figshare.com/articles/' in url:
                # 直接URL模式: https://figshare.com/articles/dataset/title/12345678
                match = re.search(r'figshare\.com/articles/(?:dataset/)?[^/]+/(\d+)', url)
                if match:
                    figshare_id = match.group(1)

            if not figshare_id:
                logger.error(f"无法从URL提取Figshare ID: {url}")
                return False, "无法从URL提取Figshare ID"

            # 使用Figshare API获取数据集信息
            api_url = f"https://api.figshare.com/v2/articles/{figshare_id}"
            logger.info(f"正在获取Figshare API数据: {api_url}")

            response = requests.get(api_url, timeout=30)
            if response.status_code != 200:
                logger.error(f"API请求失败: HTTP {response.status_code}")
                return self._download_with_selenium(url, name)

            data = response.json()

            # 创建数据集目录
            download_dir = os.path.join(self.downloader_config.get('download_dir', 'datasets'),
                                        self._sanitize_filename(name))
            os.makedirs(download_dir, exist_ok=True)

            # 保存元数据
            with open(os.path.join(download_dir, 'metadata.json'), 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            # 下载文件
            files = data.get('files', [])
            if not files:
                logger.warning("API返回的数据集没有文件")
                return self._download_with_selenium(url, name)

            logger.info(f"找到 {len(files)} 个文件")

            success_count = 0
            for i, file_info in enumerate(files):
                try:
                    file_name = file_info.get('name', f'file_{i}.bin')
                    file_name = self._sanitize_filename(file_name)
                    file_path = os.path.join(download_dir, file_name)

                    # 获取下载URL
                    download_url = file_info.get('download_url')
                    if not download_url:
                        logger.warning(f"文件 {file_name} 没有下载链接")
                        continue

                    logger.info(f"正在下载文件 {i + 1}/{len(files)}: {file_name}")

                    # 下载文件
                    response = requests.get(download_url, stream=True, timeout=600)
                    if response.status_code != 200:
                        logger.error(f"文件下载失败: HTTP {response.status_code}")
                        continue

                    # 获取文件大小
                    total_size = int(response.headers.get('content-length', 0))

                    # 保存文件
                    with open(file_path, 'wb') as f:
                        downloaded = 0
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                                # 打印进度
                                if total_size > 0 and downloaded % (1024 * 1024) == 0:  # 每1MB
                                    percent = (downloaded / total_size) * 100
                                    logger.info(
                                        f"下载进度: {percent:.1f}% ({downloaded / (1024 * 1024):.1f} MB / {total_size / (1024 * 1024):.1f} MB)")

                    success_count += 1
                    logger.info(f"文件 {file_name} 下载完成")

                except Exception as e:
                    logger.error(f"下载文件时出错: {e}")

            if success_count > 0:
                logger.info(f"成功下载 {success_count} 个文件到 {download_dir}")
                return True, f"成功下载 {success_count}/{len(files)} 个文件"
            else:
                logger.error("所有文件下载失败")
                return self._download_with_selenium(url, name)

        except Exception as e:
            logger.error(f"Figshare API下载出错: {e}")
            # 失败时回退到Selenium
            return self._download_with_selenium(url, name)

    def _download_with_selenium(self, url, name):
        """使用Selenium下载数据集"""
        try:
            # 动态导入Selenium
            try:
                from selenium import webdriver
                from selenium.webdriver.chrome.options import Options
                from selenium.webdriver.common.by import By
                from selenium.webdriver.support.ui import WebDriverWait
                from selenium.webdriver.support import expected_conditions as EC
                from selenium.common.exceptions import TimeoutException
            except ImportError:
                return False, "未安装Selenium，无法使用浏览器下载 (pip install selenium)"

            # 准备下载目录
            download_dir = os.path.join(self.downloader_config.get('download_dir', 'datasets'),
                                        self._sanitize_filename(name))
            os.makedirs(download_dir, exist_ok=True)

            # 设置Chrome选项
            chrome_options = Options()
            # chrome_options.add_argument("--headless")  # 无头模式（调试时可注释掉）
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")

            # 设置下载目录
            prefs = {
                "download.default_directory": download_dir,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": False
            }
            chrome_options.add_experimental_option("prefs", prefs)

            logger.info(f"启动浏览器下载: {url}")

            # 初始化WebDriver
            driver = webdriver.Chrome(options=chrome_options)

            try:
                driver.get(url)

                # 等待页面加载
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )

                # 保存页面源码和截图
                driver.save_screenshot(os.path.join(download_dir, "screenshot.png"))
                with open(os.path.join(download_dir, "page_source.html"), "w", encoding="utf-8") as f:
                    f.write(driver.page_source)

                # 查找下载按钮
                download_buttons = []

                # 常见的下载按钮选择器
                selectors = [
                    # 特定网站选择器
                    '[data-test="download"]',
                    '[data-url-type="download"]',
                    '.download-button',
                    '.download-all',
                    # 通用选择器
                    'a[download]',
                    'a.download',
                    'a[href$=".zip"]',
                    'a[href$=".tar.gz"]',
                    'a[href$=".csv"]',
                ]

                # 尝试所有CSS选择器
                for selector in selectors:
                    try:
                        elements = driver.find_elements(By.CSS_SELECTOR, selector)
                        for element in elements:
                            if element.is_displayed() and element.is_enabled():
                                download_buttons.append(element)
                    except:
                        pass

                # 尝试XPath选择器
                xpath_selectors = [
                    '//*[contains(translate(text(), "DOWNLOAD", "download"), "download")]',
                    '//a[contains(translate(text(), "DOWNLOAD", "download"), "download")]',
                    '//button[contains(translate(text(), "DOWNLOAD", "download"), "download")]'
                ]

                for selector in xpath_selectors:
                    try:
                        elements = driver.find_elements(By.XPATH, selector)
                        for element in elements:
                            if element.is_displayed() and element.is_enabled():
                                download_buttons.append(element)
                    except:
                        pass

                # 如果找到下载按钮，点击它们
                if download_buttons:
                    logger.info(f"找到 {len(download_buttons)} 个下载按钮")

                    # 点击下载按钮
                    downloads_initiated = 0
                    for button in download_buttons[:3]:  # 最多点击前3个按钮
                        try:
                            logger.info(f"点击下载按钮: {button.text or '未命名按钮'}")
                            # 滚动到按钮位置
                            driver.execute_script("arguments[0].scrollIntoView();", button)
                            time.sleep(1)
                            button.click()
                            time.sleep(3)  # 等待下载开始
                            downloads_initiated += 1
                        except Exception as e:
                            logger.error(f"点击按钮出错: {e}")

                    # 等待下载完成
                    if downloads_initiated > 0:
                        logger.info("等待下载完成...")
                        time.sleep(30)  # 等待下载

                        # 检查下载的文件
                        files = [f for f in os.listdir(download_dir)
                                 if f not in ["page_source.html", "screenshot.png"]
                                 and not f.endswith(".crdownload")]

                        if files:
                            logger.info(f"成功下载 {len(files)} 个文件: {', '.join(files)}")
                            return True, f"成功下载 {len(files)} 个文件"
                        else:
                            logger.warning("未找到下载文件，可能下载中或失败")
                            return True, "已启动下载，但未找到完成的文件，请检查下载目录"
                    else:
                        logger.warning("未能点击任何下载按钮")
                        return False, "未能点击任何下载按钮"
                else:
                    logger.warning("未找到下载按钮")
                    return False, "未找到下载按钮"

            finally:
                # 关闭浏览器
                driver.quit()

        except Exception as e:
            logger.error(f"Selenium下载出错: {e}")
            return False, f"浏览器下载失败: {str(e)}"

    def _sanitize_filename(self, filename):
        """清理文件名"""
        # 移除非法字符
        illegal_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
        for char in illegal_chars:
            filename = filename.replace(char, '_')

        # 限制长度
        if len(filename) > 100:
            filename = filename[:100]

        return filename

    def extract_datasets_from_paper(self, paper):
        """
        从论文中提取数据集信息

        Args:
            paper: 论文字典

        Returns:
            list: 提取的数据集列表
        """
        if not paper:
            return []

        datasets = []

        # 直接获取论文中的数据集
        if 'datasets' in paper and paper['datasets']:
            datasets = paper['datasets']

        # 添加论文信息
        for dataset in datasets:
            dataset['paper_title'] = paper.get('title', '')
            dataset['paper_url'] = paper.get('url', '')
            dataset['paper_doi'] = paper.get('doi', '')

        return datasets