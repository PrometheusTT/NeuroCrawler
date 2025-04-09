#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import time
import logging
import argparse
import requests
from urllib.parse import urlparse

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, NoSuchElementException

    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("figshare_downloader.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class FigshareDownloader:
    """专门处理Figshare数据集下载的工具"""

    def __init__(self, download_dir=None):
        """
        初始化下载器

        Args:
            download_dir: 下载目录
        """
        self.download_dir = download_dir or os.path.join(os.getcwd(), 'figshare_downloads')
        os.makedirs(self.download_dir, exist_ok=True)
        self.driver = None

    def extract_figshare_id(self, url):
        """从URL中提取Figshare ID"""
        try:
            # 尝试不同的URL模式
            if '10.6084/m9.figshare.' in url:
                # DOI模式: https://doi.org/10.6084/m9.figshare.12345678
                match = re.search(r'10\.6084/m9\.figshare\.(\d+)', url)
                if match:
                    return match.group(1)
            elif 'figshare.com/articles/' in url:
                # 直接URL模式: https://figshare.com/articles/dataset/title/12345678
                match = re.search(r'figshare\.com/articles/(?:dataset/)?[^/]+/(\d+)', url)
                if match:
                    return match.group(1)
        except Exception as e:
            logger.error(f"提取Figshare ID失败: {e}")

        return None

    def download_with_api(self, url):
        """
        使用Figshare API下载数据集

        Args:
            url: Figshare URL

        Returns:
            bool: 是否成功
            str: 消息
        """
        figshare_id = self.extract_figshare_id(url)
        if not figshare_id:
            logger.error(f"无法从URL提取Figshare ID: {url}")
            return False, "无法从URL提取Figshare ID"

        try:
            # 使用Figshare API获取数据集信息
            api_url = f"https://api.figshare.com/v2/articles/{figshare_id}"
            logger.info(f"正在获取Figshare API数据: {api_url}")

            response = requests.get(api_url, timeout=30)
            if response.status_code != 200:
                logger.error(f"API请求失败: HTTP {response.status_code}")
                return False, f"API请求失败: HTTP {response.status_code}"

            data = response.json()

            # 创建数据集目录
            dataset_name = data.get('title', f'figshare_{figshare_id}')
            dataset_name = self._sanitize_filename(dataset_name)
            dataset_dir = os.path.join(self.download_dir, dataset_name)
            os.makedirs(dataset_dir, exist_ok=True)

            # 保存元数据
            with open(os.path.join(dataset_dir, 'metadata.json'), 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            # 下载文件
            files = data.get('files', [])
            if not files:
                logger.warning("API返回的数据集没有文件")
                return False, "API返回的数据集没有文件"

            logger.info(f"找到 {len(files)} 个文件")

            success_count = 0
            for i, file_info in enumerate(files):
                try:
                    file_name = file_info.get('name', f'file_{i}.bin')
                    file_name = self._sanitize_filename(file_name)
                    file_path = os.path.join(dataset_dir, file_name)

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
                logger.info(f"成功下载 {success_count} 个文件到 {dataset_dir}")
                return True, f"成功下载 {success_count}/{len(files)} 个文件"
            else:
                logger.error("所有文件下载失败")
                return False, "所有文件下载失败"

        except Exception as e:
            logger.error(f"API下载出错: {e}")
            return False, f"API下载出错: {str(e)}"

    def download_with_selenium(self, url):
        """
        使用Selenium下载数据集

        Args:
            url: Figshare URL

        Returns:
            bool: 是否成功
            str: 消息
        """
        if not SELENIUM_AVAILABLE:
            logger.error("Selenium未安装，无法使用浏览器下载")
            return False, "Selenium未安装，无法使用浏览器下载 (pip install selenium)"

        try:
            # 为当前URL创建一个唯一的下载目录
            parsed_url = urlparse(url)
            url_path = parsed_url.path.strip('/')
            folder_name = url_path.replace('/', '_') or 'figshare'
            download_dir = os.path.join(self.download_dir, self._sanitize_filename(folder_name))
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

            logger.info(f"正在初始化Chrome浏览器...")

            # 初始化WebDriver
            self.driver = webdriver.Chrome(options=chrome_options)

            try:
                logger.info(f"正在访问 {url}")
                self.driver.get(url)

                # 等待页面加载
                WebDriverWait(self.driver, 30).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )

                # 保存页面源码，以便分析
                html_path = os.path.join(download_dir, "page_source.html")
                with open(html_path, 'w', encoding='utf-8') as f:
                    f.write(self.driver.page_source)

                # 获取页面标题
                title = self.driver.title
                logger.info(f"页面标题: {title}")

                # 保存页面截图
                screenshot_path = os.path.join(download_dir, "screenshot.png")
                self.driver.save_screenshot(screenshot_path)
                logger.info(f"已保存页面截图: {screenshot_path}")

                # 查找并点击下载按钮
                download_buttons = self._find_download_buttons()

                if not download_buttons:
                    logger.warning("未找到下载按钮")
                    return False, "未找到下载按钮"

                # 点击所有找到的下载按钮
                downloads_initiated = 0

                for button in download_buttons[:3]:  # 最多尝试前3个按钮
                    try:
                        logger.info(f"点击下载按钮: {button.text or '未命名按钮'}")
                        # 滚动到按钮位置
                        self.driver.execute_script("arguments[0].scrollIntoView();", button)
                        time.sleep(1)
                        button.click()
                        time.sleep(3)  # 等待下载开始
                        downloads_initiated += 1
                    except Exception as e:
                        logger.error(f"点击下载按钮时出错: {e}")

                # 等待下载完成
                wait_time = 120  # 等待时间（秒）
                logger.info(f"等待下载完成，最长等待时间: {wait_time}秒")
                for _ in range(wait_time):
                    # 检查下载目录中是否有新文件
                    files = [f for f in os.listdir(download_dir) if f not in ["page_source.html", "screenshot.png"]]
                    if files and not any(f.endswith('.crdownload') for f in files):
                        logger.info(f"下载完成，找到 {len(files)} 个文件")
                        break
                    time.sleep(1)

                # 最终检查下载的文件
                files = [f for f in os.listdir(download_dir) if f not in ["page_source.html", "screenshot.png"]]
                complete_files = [f for f in files if not f.endswith('.crdownload')]

                if complete_files:
                    logger.info(f"成功下载 {len(complete_files)} 个文件: {', '.join(complete_files)}")
                    return True, f"成功下载 {len(complete_files)} 个文件"
                elif downloads_initiated > 0:
                    logger.warning("下载已启动但尚未完成，请检查下载目录")
                    return True, "下载已启动但可能尚未完成，请检查下载目录"
                else:
                    logger.error("未能下载任何文件")
                    return False, "未能下载任何文件"

            finally:
                if self.driver:
                    self.driver.quit()
                    self.driver = None

        except Exception as e:
            logger.error(f"Selenium下载出错: {e}")
            if self.driver:
                self.driver.quit()
                self.driver = None
            return False, f"Selenium下载出错: {str(e)}"

    def _find_download_buttons(self):
        """查找页面中的下载按钮"""
        if not self.driver:
            return []

        download_buttons = []

        # 常见的下载按钮选择器
        selectors = [
            # Figshare特定选择器
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
            'button:contains("Download")',
            'a:contains("Download")'
        ]

        for selector in selectors:
            try:
                # CSS选择器
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    if element.is_displayed() and element.is_enabled():
                        download_buttons.append(element)
            except:
                pass

        # 根据文本内容查找
        try:
            elements = self.driver.find_elements(By.XPATH,
                                                 '//*[contains(translate(text(), "DOWNLOAD", "download"), "download")]')
            for element in elements:
                if element.is_displayed() and element.is_enabled():
                    download_buttons.append(element)
        except:
            pass

        return download_buttons

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

    def download(self, url):
        """
        下载Figshare数据集，首先尝试API，如果失败则使用Selenium

        Args:
            url: Figshare URL

        Returns:
            bool: 是否成功
            str: 消息
        """
        logger.info(f"开始下载Figshare数据集: {url}")

        # 首先尝试使用API
        api_success, api_message = self.download_with_api(url)
        if api_success:
            return True, api_message

        logger.info(f"API下载失败: {api_message}，尝试使用Selenium")

        # 如果API失败，尝试使用Selenium
        selenium_success, selenium_message = self.download_with_selenium(url)
        return selenium_success, selenium_message


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='Figshare数据集下载工具')
    parser.add_argument('--url', type=str, required=True, help='Figshare数据集URL')
    parser.add_argument('--download-dir', type=str, default=None, help='下载目录')
    args = parser.parse_args()

    downloader = FigshareDownloader(args.download_dir)
    success, message = downloader.download(args.url)

    if success:
        logger.info(f"下载成功: {message}")
        print(f"数据集下载成功: {message}")
    else:
        logger.error(f"下载失败: {message}")
        print(f"数据集下载失败: {message}")


if __name__ == "__main__":
    main()