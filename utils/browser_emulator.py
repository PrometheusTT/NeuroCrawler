#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
import requests

logger = logging.getLogger(__name__)


class BrowserEmulator:
    """模拟浏览器行为的工具类，支持普通请求和Selenium渲染"""

    def __init__(self):
        self.session = requests.Session()
        self._driver = None
        self.default_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
        }

    def _initialize_driver(self):
        """初始化Selenium WebDriver"""
        if self._driver is None:
            try:
                chrome_options = Options()
                # 必要的无头模式选项
                chrome_options.add_argument('--headless')
                chrome_options.add_argument('--no-sandbox')
                chrome_options.add_argument('--disable-dev-shm-usage')
                chrome_options.add_argument('--disable-gpu')

                # 模拟真实浏览器环境
                chrome_options.add_argument('--window-size=1920,1080')
                chrome_options.add_argument(f'user-agent={self.default_headers["User-Agent"]}')
                chrome_options.add_argument('--disable-blink-features=AutomationControlled')

                # 禁用图片加载提升速度
                chrome_options.add_argument('--blink-settings=imagesEnabled=false')

                # 添加实验性选项，降低被检测几率
                chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
                chrome_options.add_experimental_option('useAutomationExtension', False)

                service = Service(ChromeDriverManager().install())
                self._driver = webdriver.Chrome(service=service, options=chrome_options)

                # 执行CDP命令消除navigator.webdriver检测
                self._driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                    'source': '''
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    })
                    '''
                })

                logger.info("Selenium WebDriver初始化成功")
            except Exception as e:
                logger.error(f"初始化WebDriver失败: {e}")
                raise

    def get_page(self, url, use_selenium=False, wait_time=10, retry_count=3, proxy=None, cookies=None,
                 additional_headers=None):
        """
        获取页面内容

        Args:
            url (str): 要请求的URL
            use_selenium (bool): 是否使用Selenium渲染页面
            wait_time (int): 等待页面加载的最大时间（秒）
            retry_count (int): 重试次数
            proxy (str): 可选的代理服务器
            cookies (dict): 可选的Cookie
            additional_headers (dict): 额外的请求头

        Returns:
            str: 页面HTML内容
        """
        if use_selenium:
            return self._get_page_with_selenium(url, wait_time, retry_count, proxy, cookies)
        else:
            return self._get_page_with_requests(url, retry_count, proxy, cookies, additional_headers)

    def _get_page_with_requests(self, url, retry_count=3, proxy=None, cookies=None, additional_headers=None):
        """使用requests库获取页面内容"""
        headers = self.default_headers.copy()
        if additional_headers:
            headers.update(additional_headers)

        proxies = None
        if proxy:
            proxies = {
                'http': proxy,
                'https': proxy
            }

        if cookies:
            self.session.cookies.update(cookies)

        for attempt in range(retry_count):
            try:
                # 添加随机延迟
                time.sleep(random.uniform(1, 3))

                response = self.session.get(
                    url,
                    headers=headers,
                    proxies=proxies,
                    timeout=30,
                    allow_redirects=True
                )

                if response.status_code == 200:
                    logger.info(f"成功请求页面: {url}")
                    return response.text
                elif response.status_code == 403 or response.status_code == 429:
                    logger.warning(f"请求被拒绝(状态码:{response.status_code}): {url}, 尝试使用Selenium")
                    # 如果被拒绝，尝试使用Selenium来绕过反爬
                    return self._get_page_with_selenium(url, 15, 1, proxy, cookies)
                else:
                    logger.warning(f"请求失败(状态码:{response.status_code}): {url}, 重试({attempt + 1}/{retry_count})")
                    time.sleep(random.uniform(5, 10))  # 失败后等待更长时间
            except requests.exceptions.RequestException as e:
                logger.error(f"请求异常: {e}, 重试({attempt + 1}/{retry_count})")
                time.sleep(random.uniform(5, 10))

        logger.error(f"在{retry_count}次尝试后仍无法获取页面: {url}")
        return None

    def _get_page_with_selenium(self, url, wait_time=10, retry_count=3, proxy=None, cookies=None):
        """使用Selenium渲染页面获取内容"""
        for attempt in range(retry_count):
            try:
                # 确保WebDriver已初始化
                self._initialize_driver()

                if proxy:
                    # 设置代理
                    self._driver.execute_cdp_cmd('Network.setUserAgentOverride',
                                                 {"userAgent": self.default_headers["User-Agent"]})
                    self._driver.execute_cdp_cmd('Network.enable', {})
                    self._driver.execute_cdp_cmd('Network.setExtraHTTPHeaders', {"headers": self.default_headers})
                    self._driver.execute_cdp_cmd('Network.setProxy', {"proxyServer": proxy})

                # 访问URL
                self._driver.get(url)

                # 添加cookie
                if cookies:
                    for name, value in cookies.items():
                        self._driver.add_cookie({'name': name, 'value': value})
                    # 刷新页面使cookie生效
                    self._driver.refresh()

                # 等待页面加载完成
                time.sleep(2)  # 基础等待

                # 智能等待: 等待页面主体内容加载
                try:
                    # 更灵活的等待条件
                    selectors = [
                        "div.c-article-body", "div#abstract", "div.article-body",
                        "div.fulltext-view", "ul.app-article-list-row",
                        "li.c-list-group__item", "div.container-type-article-list"
                    ]

                    # 使用多个选择器中的任意一个
                    conditions = [
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                        for selector in selectors
                    ]

                    # 等待任何一个元素出现
                    for _ in range(wait_time):
                        for condition in conditions:
                            try:
                                if condition(self._driver):
                                    break
                            except:
                                pass
                        else:
                            # 如果所有条件都不满足，等待1秒后重试
                            time.sleep(1)
                            continue
                        # 如果找到了元素，跳出循环
                        break
                except Exception as e:
                    logger.warning(f"等待页面内容时出错: {e}, 继续处理")

                    # 模拟更自然的滚动
                self._realistic_scroll()

                # 获取页面源码
                html_content = self._driver.page_source

                logger.info(f"成功使用Selenium获取页面: {url}")
                return html_content

            except (WebDriverException, Exception) as e:
                logger.error(f"Selenium获取页面失败: {e}, 重试({attempt + 1}/{retry_count})")

                # 重置WebDriver
                if self._driver:
                    try:
                        self._driver.quit()
                    except:
                        pass
                    self._driver = None

                time.sleep(random.uniform(5, 15))

        logger.error(f"在{retry_count}次尝试后仍无法使用Selenium获取页面: {url}")
        return None

    def _realistic_scroll(self):
        """
        模拟真实用户的滚动行为
        包括随机停顿、变速滚动等
        """
        if not self._driver:
            return

        try:
            # 获取页面高度
            total_height = self._driver.execute_script("return document.body.scrollHeight")
            viewport_height = self._driver.execute_script("return window.innerHeight")

            if not total_height or total_height <= viewport_height:
                return  # 页面太短，不需要滚动

            # 计算滚动次数 (基于页面高度，但不超过5-8次)
            scroll_count = min(random.randint(5, 8), max(2, total_height // viewport_height))

            # 初始位置
            current_position = 0

            # 模拟人类滚动行为
            for i in range(scroll_count):
                # 随机决定滚动距离 (根据视口高度的百分比)
                scroll_ratio = random.uniform(0.7, 1.3)
                scroll_distance = int(viewport_height * scroll_ratio)

                # 确保最后一次滚动能到底部
                if i == scroll_count - 1:
                    new_position = total_height
                else:
                    new_position = min(current_position + scroll_distance, total_height)

                # 使用平滑滚动
                self._driver.execute_script(f"window.scrollTo({{top: {new_position}, behavior: 'smooth'}});")
                current_position = new_position

                # 随机暂停，模拟阅读
                time.sleep(random.uniform(0.5, 2.0))

            # 有时回到顶部，有时停在中间位置
            if random.random() < 0.7:  # 70%的概率回到顶部
                self._driver.execute_script("window.scrollTo({top: 0, behavior: 'smooth'});")
                time.sleep(random.uniform(0.3, 0.7))

        except Exception as e:
            logger.warning(f"模拟滚动时出错: {e}")
    def __del__(self):
        """确保退出时关闭WebDriver"""
        if self._driver:
            try:
                self._driver.quit()
            except:
                pass