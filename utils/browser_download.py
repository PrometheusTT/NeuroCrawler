#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

logger = logging.getLogger(__name__)


class BrowserDownloader:
    """使用浏览器模拟下载复杂网站的数据集"""

    def __init__(self, download_dir=None):
        self.download_dir = download_dir or os.path.join(os.getcwd(), 'downloads')
        os.makedirs(self.download_dir, exist_ok=True)

    def download_with_browser(self, url, dataset_name=None):
        """
        使用浏览器模拟下载数据集

        Args:
            url: 数据集URL
            dataset_name: 数据集名称

        Returns:
            bool: 是否成功
            str: 消息或错误信息
        """
        driver = None
        try:
            logger.info(f"使用浏览器模拟下载: {url}")

            # 配置Chrome选项
            chrome_options = Options()
            chrome_options.add_argument("--headless")  # 无界面模式
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")

            # 设置下载目录
            prefs = {
                "download.default_directory": self.download_dir,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": False
            }
            chrome_options.add_experimental_option("prefs", prefs)

            # 初始化WebDriver
            driver = webdriver.Chrome(options=chrome_options)

            # 访问URL
            driver.get(url)

            # 等待页面加载
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            # 识别网站类型并处理
            if "figshare.com" in url or "10.6084/m9.figshare" in url:
                return self._handle_figshare(driver, url)
            elif "zenodo.org" in url:
                return self._handle_zenodo(driver, url)
            elif "osf.io" in url:
                return self._handle_osf(driver, url)
            elif "nature.com" in url:
                return self._handle_nature(driver, url)
            else:
                # 通用处理
                return self._handle_generic(driver, url)

        except Exception as e:
            logger.error(f"浏览器模拟下载出错: {e}")
            return False, f"浏览器模拟下载出错: {str(e)}"

        finally:
            if driver:
                driver.quit()

    def _handle_figshare(self, driver, url):
        """处理Figshare数据集"""
        try:
            # 等待下载按钮出现
            download_buttons = WebDriverWait(driver, 20).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR,
                                                     "[data-test='download'], .download-button, .download-all"))
            )

            if not download_buttons:
                return False, "未找到Figshare下载按钮"

            # 点击第一个下载按钮
            download_buttons[0].click()

            # 等待下载开始和完成
            time.sleep(5)  # 简单等待，实际应该检测下载状态

            return True, "已触发Figshare数据集下载"

        except TimeoutException:
            logger.error("等待Figshare下载按钮超时")
            return False, "等待Figshare下载按钮超时"
        except Exception as e:
            logger.error(f"处理Figshare网站时出错: {e}")
            return False, f"处理Figshare网站时出错: {str(e)}"

    # 其他平台的处理方法...
    def _handle_zenodo(self, driver, url):
        """处理Zenodo数据集"""
        # 类似Figshare的处理逻辑
        pass

    def _handle_osf(self, driver, url):
        """处理OSF数据集"""
        # 针对OSF的处理逻辑
        pass

    def _handle_nature(self, driver, url):
        """处理Nature数据集"""
        # 针对Nature的处理逻辑
        pass

    def _handle_generic(self, driver, url):
        """通用网站处理"""
        try:
            # 查找常见的下载按钮
            download_selectors = [
                "a[download]",
                "a.download",
                "a[href$='.zip']",
                "a[href$='.tar.gz']",
                "a:contains('Download')",
                "button:contains('Download')"
            ]

            for selector in download_selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        elements[0].click()
                        time.sleep(3)  # 等待下载开始
                        return True, f"已触发下载 (使用选择器: {selector})"
                except:
                    continue

            # 如果没有找到下载按钮，截图保存
            screenshot_path = os.path.join(self.download_dir, "page_screenshot.png")
            driver.save_screenshot(screenshot_path)

            # 保存页面源码
            html_path = os.path.join(self.download_dir, "page_source.html")
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(driver.page_source)

            return False, "未找到下载按钮，已保存页面截图和源码"

        except Exception as e:
            logger.error(f"通用网站处理出错: {e}")
            return False, f"通用网站处理出错: {str(e)}"