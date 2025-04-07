#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import random
import time
import requests
import json
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class ProxyManager:
    """代理IP管理器，提供轮换代理功能"""

    def __init__(self, config=None):
        self.config = config or {}
        self.proxies = []
        self.last_update = None
        self.update_interval = timedelta(minutes=self.config.get('update_interval_minutes', 30))

        # 初始化代理列表
        self._update_proxies()

    def _update_proxies(self):
        """更新代理列表"""
        current_time = datetime.now()

        # 如果上次更新时间在更新间隔内，则不更新
        if self.last_update and (current_time - self.last_update) < self.update_interval:
            return

        try:
            if 'proxy_api_url' in self.config:
                # 从API获取代理
                response = requests.get(
                    self.config['proxy_api_url'],
                    headers={'User-Agent': 'Mozilla/5.0'},
                    timeout=10
                )

                if response.status_code == 200:
                    data = response.json()

                    # 根据API返回格式处理
                    if 'data' in data and isinstance(data['data'], list):
                        self.proxies = [f"{p['ip']}:{p['port']}" for p in data['data']]
                    elif isinstance(data, list):
                        self.proxies = [f"{p['ip']}:{p['port']}" for p in data]
                    else:
                        # 尝试解析其他格式
                        proxies = []
                        for key, value in data.items():
                            if isinstance(value, dict) and 'ip' in value and 'port' in value:
                                proxies.append(f"{value['ip']}:{value['port']}")

                        if proxies:
                            self.proxies = proxies

            # 如果配置了静态代理列表或API获取失败，使用静态列表
            if not self.proxies and 'proxy_list' in self.config:
                self.proxies = self.config['proxy_list']

            # 记录更新时间
            self.last_update = current_time
            logger.info(f"更新代理列表成功，获取到 {len(self.proxies)} 个代理")

        except Exception as e:
            logger.error(f"更新代理列表失败: {e}")

            # 如果更新失败且没有现有代理，使用静态列表
            if not self.proxies and 'proxy_list' in self.config:
                self.proxies = self.config['proxy_list']

    def get_proxy(self):
        """获取一个代理"""
        # 确保代理列表是最新的
        self._update_proxies()

        # 如果有代理则随机返回一个
        if self.proxies:
            return random.choice(self.proxies)
        else:
            return None

    def get_random_proxies(self, count=3):
        """获取多个随机代理"""
        # 确保代理列表是最新的
        self._update_proxies()

        if not self.proxies:
            return []

        # 如果请求的数量超过可用代理数量，返回所有代理
        if count >= len(self.proxies):
            return self.proxies.copy()

        # 否则返回随机选择的代理
        return random.sample(self.proxies, count)

    def report_bad_proxy(self, proxy):
        """报告不工作的代理"""
        if proxy in self.proxies:
            self.proxies.remove(proxy)
            logger.info(f"已从代理列表中移除不工作的代理: {proxy}")