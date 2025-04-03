#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import time
import random
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from fake_useragent import UserAgent

logger = logging.getLogger(__name__)


class BrowserEmulator:
    """
    浏览器模拟器，用于模拟真实用户行为爬取动态内容网站
    支持JavaScript渲染的页面抓取
    """

    def __init__(self, headless=True, proxy=None):
        self.headless = headless
        self.proxy = proxy
        self.ua = UserAgent()
        self.driver = None
        self.session = requests.Session()
        self.session_headers = {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
        }
        self.session.headers.update(self.session_headers)

    def _initialize_driver(self):
        """初始化Selenium WebDriver"""
        if self.driver:
            return

        try:
            chrome_options = Options()

            if self.headless:
                chrome_options.add_argument('--headless')

            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument(f'--user-agent={self.ua.random}')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
            chrome_options.add_experimental_option('useAutomationExtension', False)

            if self.proxy:
                chrome_options.add_argument(f'--proxy-server={self.proxy}')

            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)

            # 设置窗口大小
            self.driver.set_window_size(1920, 1080)

            # 绕过WebDriver检测
            self.driver.execute_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )

            logger.info("Selenium WebDriver初始化成功")

        except Exception as e:
            logger.error(f"初始化WebDriver时出错: {e}")
            raise

    def close_driver(self):
        """关闭WebDriver"""
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None

    def get_page(self, url, use_selenium=False, wait_time=10, retry=3):
        """获取页面内容"""
        if use_selenium:
            return self._get_with_selenium(url, wait_time, retry)
        else:
            return self._get_with_requests(url, retry)

    def _get_with_requests(self, url, retry=3):
        """使用requests获取页面内容"""
        attempt = 0

        while attempt < retry:
            try:
                # 更新User-Agent
                self.session.headers.update({'User-Agent': self.ua.random})

                response = self.session.get(
                    url,
                    timeout=30,
                    proxies=self.proxy if self.proxy else None
                )

                if response.status_code == 200:
                    return response.text
                elif response.status_code == 403:
                    logger.warning(f"访问被拒绝(403): {url}")
                    time.sleep(random.uniform(5, 10))
                elif response.status_code == 429:
                    logger.warning(f"请求过多(429): {url}")
                    time.sleep(random.uniform(60, 120))
                else:
                    logger.warning(f"HTTP错误: {response.status_code}, URL: {url}")

            except Exception as e:
                logger.error(f"请求出错: {e}, URL: {url}")

            attempt += 1
            time.sleep(random.uniform(2, 5))

        return None

    def _get_with_selenium(self, url, wait_time=10, retry=3):
        """使用Selenium获取页面内容"""
        self._initialize_driver()
        attempt = 0

        while attempt < retry:
            try:
                self.driver.get(url)

                # 随机等待一段时间，模拟人类行为
                time.sleep(random.uniform(2, 5))

                # 等待页面加载完成
                WebDriverWait(self.driver, wait_time).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )

                # 模拟滚动页面
                self._scroll_page()

                # 返回页面源代码
                return self.driver.page_source

            except TimeoutException:
                logger.warning(f"页面加载超时: {url}")
            except WebDriverException as e:
                logger.error(f"WebDriver错误: {e}, URL: {url}")
            except Exception as e:
                logger.error(f"Selenium获取页面出错: {e}, URL: {url}")

            attempt += 1
            time.sleep(random.uniform(5, 10))

        return None

    def _scroll_page(self, pause_time=0.5):
        """模拟页面滚动"""
        try:
            # 获取页面总高度
            total_height = self.driver.execute_script("return document.body.scrollHeight")

            # 分段滚动
            height = 0
            scroll_step = random.randint(300, 700)  # 每次滚动的随机像素

            while height < total_height:
                # 随机滚动距离
                next_height = min(height + scroll_step, total_height)

                # 执行滚动
                self.driver.execute_script(f"window.scrollTo({height}, {next_height});")
                height = next_height

                # 随机等待
                time.sleep(random.uniform(0.3, 0.7) * pause_time)

                # 有小概率暂停一下，更像人类行为
                if random.random() < 0.1:
                    time.sleep(random.uniform(1, 2))

            # 滚动回顶部
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(pause_time)

        except Exception as e:
            logger.error(f"滚动页面时出错: {e}")

    def __del__(self):
        """析构函数，确保关闭WebDriver"""
        self.close_driver()