#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import time
import random
import requests
from threading import Lock

logger = logging.getLogger(__name__)


class ProxyManager:
    """
    代理管理器，用于获取和验证代理
    支持多种代理源
    """

    def __init__(self, config=None):
        self.config = config or {}
        self.proxies = []
        self.working_proxies = []
        self.last_update = 0
        self.update_interval = self.config.get('update_interval', 3600)  # 默认1小时更新一次
        self.lock = Lock()
        self.test_url = self.config.get('test_url', 'https://www.google.com')
        self.timeout = self.config.get('timeout', 10)

        # 初始化代理列表
        if self.config.get('auto_init', True):
            self.update_proxies()

    def update_proxies(self):
        """更新代理列表"""
        with self.lock:
            now = time.time()

            # 如果距离上次更新时间不足更新间隔，则跳过
            if now - self.last_update < self.update_interval and self.working_proxies:
                return

            self.proxies = []

            # 从各个来源获取代理
            for source_name, source_config in self.config.get('sources', {}).items():
                try:
                    logger.info(f"从 {source_name} 获取代理")
                    source_proxies = self._get_proxies_from_source(source_name, source_config)
                    self.proxies.extend(source_proxies)
                    logger.info(f"从 {source_name} 获取了 {len(source_proxies)} 个代理")
                except Exception as e:
                    logger.error(f"从 {source_name} 获取代理时出错: {e}")

            # 验证代理
            self.working_proxies = self._validate_proxies(self.proxies)
            logger.info(f"验证了 {len(self.proxies)} 个代理，其中 {len(self.working_proxies)} 个可用")

            self.last_update = now

    def _get_proxies_from_source(self, source_name, source_config):
        """从指定来源获取代理"""
        proxies = []

        if source_name == 'free_proxy_list':
            # 从free-proxy-list.net获取免费代理
            url = source_config.get('url', 'https://free-proxy-list.net/')
            try:
                response = requests.get(url, timeout=self.timeout)
                if response.status_code == 200:
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(response.text, 'html.parser')
                    table = soup.find('table', {'id': 'proxylisttable'})

                    if table:
                        for row in table.tbody.find_all('tr'):
                            cols = row.find_all('td')
                            if len(cols) >= 7:
                                ip = cols[0].text.strip()
                                port = cols[1].text.strip()
                                https = cols[6].text.strip() == 'yes'

                                proxy = {
                                    'ip': ip,
                                    'port': port,
                                    'protocol': 'https' if https else 'http'
                                }
                                proxies.append(proxy)
            except Exception as e:
                logger.error(f"从free-proxy-list获取代理时出错: {e}")

        elif source_name == 'api_provider':
            # 从API提供商获取代理
            url = source_config.get('url')
            api_key = source_config.get('api_key')

            if url and api_key:
                try:
                    response = requests.get(
                        url,
                        params={'api_key': api_key},
                        timeout=self.timeout
                    )

                    if response.status_code == 200:
                        data = response.json()
                        if 'proxies' in data:
                            for proxy_data in data['proxies']:
                                proxy = {
                                    'ip': proxy_data.get('ip'),
                                    'port': proxy_data.get('port'),
                                    'protocol': proxy_data.get('protocol', 'http'),
                                    'username': proxy_data.get('username'),
                                    'password': proxy_data.get('password')
                                }
                                proxies.append(proxy)
                except Exception as e:
                    logger.error(f"从API提供商获取代理时出错: {e}")

        elif source_name == 'custom_list':
            # 使用自定义代理列表
            custom_proxies = source_config.get('proxies', [])
            for proxy_data in custom_proxies:
                proxies.append(proxy_data)

        return proxies

    def _validate_proxies(self, proxies, max_workers=10):
        """验证代理是否可用"""
        working_proxies = []

        # 使用线程池并行验证
        try:
            from concurrent.futures import ThreadPoolExecutor

            def validate_proxy(proxy):
                if self._test_proxy(proxy):
                    return proxy
                return None

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                results = list(executor.map(validate_proxy, proxies))

            working_proxies = [p for p in results if p is not None]

        except ImportError:
            # 如果没有concurrent.futures，退回到顺序验证
            for proxy in proxies:
                if self._test_proxy(proxy):
                    working_proxies.append(proxy)

        return working_proxies

    def _test_proxy(self, proxy):
        """测试单个代理是否可用"""
        proxy_url = self._format_proxy_url(proxy)
        proxies = {
            'http': proxy_url,
            'https': proxy_url
        }

        try:
            response = requests.get(
                self.test_url,
                proxies=proxies,
                timeout=self.timeout,
                verify=False
            )
            return response.status_code == 200
        except Exception:
            return False

    def _format_proxy_url(self, proxy):
        """格式化代理URL"""
        protocol = proxy.get('protocol', 'http')
        ip = proxy.get('ip')
        port = proxy.get('port')
        username = proxy.get('username')
        password = proxy.get('password')

        if not ip or not port:
            return None

        if username and password:
            return f"{protocol}://{username}:{password}@{ip}:{port}"
        else:
            return f"{protocol}://{ip}:{port}"

    def get_proxy(self, protocol=None):
        """获取一个可用代理"""
        # 如果没有可用代理或者已经过期，更新代理列表
        if not self.working_proxies or time.time() - self.last_update > self.update_interval:
            self.update_proxies()

        # 如果仍然没有可用代理，返回None
        if not self.working_proxies:
            return None

        # 根据协议筛选代理
        candidates = self.working_proxies
        if protocol:
            candidates = [p for p in candidates if p.get('protocol') == protocol]

        # 如果没有符合条件的代理，返回None
        if not candidates:
            return None

        # 随机选择一个代理
        proxy = random.choice(candidates)
        proxy_url = self._format_proxy_url(proxy)

        return {
            'http': proxy_url,
            'https': proxy_url
        }