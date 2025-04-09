#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import json
import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from urllib.parse import quote_plus
import hmac
import hashlib
import base64
import time

logger = logging.getLogger(__name__)


class Notifier:
    """通知系统，支持邮件、钉钉、WebHook等多种通知方式"""

    def __init__(self, config):
        self.config = config
        self.disabled = config.get('disabled', False)

        # 配置邮件通知
        self.email_config = config.get('email', {})

        # 配置钉钉通知
        self.dingtalk_config = config.get('dingtalk', {})

        # 配置WebHook通知
        self.webhook_config = config.get('webhook', {})

        # 配置Slack通知
        self.slack_config = config.get('slack', {})

        # 配置Discord通知
        self.discord_config = config.get('discord', {})

        # 配置企业微信通知
        self.wechat_config = config.get('wechat', {})

        # 通知频率控制
        self.last_notification_time = {}
        self.notification_threshold = config.get('threshold', {})

    def _should_notify(self, notification_type, count):
        """判断是否应该发送通知"""
        # 如果通知被禁用
        if self.disabled:
            return False

        # 获取此通知类型的配置阈值
        threshold = self.notification_threshold.get(notification_type, 1)

        # 如果数量小于阈值，不通知
        if count < threshold:
            return False

        # 检查通知频率限制
        cooldown_minutes = self.notification_threshold.get('cooldown_minutes', 60)
        now = datetime.now()

        if notification_type in self.last_notification_time:
            last_time = self.last_notification_time[notification_type]
            elapsed_minutes = (now - last_time).total_seconds() / 60

            if elapsed_minutes < cooldown_minutes:
                logger.info(f"{notification_type}通知冷却中，上次通知在{elapsed_minutes:.1f}分钟前")
                return False

        # 更新最后通知时间
        self.last_notification_time[notification_type] = now
        return True

    def _send_email(self, subject, html_content, recipients=None):
        """发送邮件通知"""
        if not self.email_config.get('enabled', False):
            return False

        try:
            smtp_server = self.email_config.get('smtp_server')
            smtp_port = self.email_config.get('smtp_port', 587)
            username = self.email_config.get('username')
            password = self.email_config.get('password')
            sender = self.email_config.get('sender', username)

            if not recipients:
                recipients = self.email_config.get('recipients', [])

            if not isinstance(recipients, list):
                recipients = [recipients]

            # 创建邮件
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = sender
            msg['To'] = ', '.join(recipients)

            # 添加HTML内容
            html_part = MIMEText(html_content, 'html')
            msg.attach(html_part)

            # 发送邮件
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.ehlo()
                use_tls = self.email_config.get('use_tls', True)
                if use_tls:
                    server.starttls()
                    server.ehlo()

                server.login(username, password)
                server.sendmail(sender, recipients, msg.as_string())

            logger.info(f"邮件通知发送成功: {subject}")
            return True

        except Exception as e:
            logger.error(f"发送邮件通知失败: {e}")
            return False

    def _send_dingtalk(self, message):
        """发送钉钉通知"""
        if not self.dingtalk_config.get('enabled', False):
            return False

        try:
            webhook_url = self.dingtalk_config.get('webhook_url')
            secret = self.dingtalk_config.get('secret')

            # 如果配置了加签
            if secret:
                timestamp = str(round(time.time() * 1000))
                string_to_sign = f"{timestamp}\n{secret}"

                # 使用HmacSHA256算法计算签名
                hmac_code = hmac.new(
                    secret.encode(),
                    string_to_sign.encode(),
                    digestmod=hashlib.sha256
                ).digest()

                # Base64 编码
                sign = quote_plus(base64.b64encode(hmac_code).decode())

                # 拼接完整URL
                webhook_url = f"{webhook_url}&timestamp={timestamp}&sign={sign}"

            # 发送请求
            headers = {'Content-Type': 'application/json; charset=utf-8'}
            response = requests.post(
                webhook_url,
                headers=headers,
                data=json.dumps(message),
                timeout=10
            )

            if response.status_code == 200 and response.json().get('errcode') == 0:
                logger.info(f"钉钉通知发送成功")
                return True
            else:
                logger.error(f"钉钉通知发送失败: {response.text}")
                return False

        except Exception as e:
            logger.error(f"发送钉钉通知失败: {e}")
            return False

    def _send_webhook(self, payload):
        """发送WebHook通知"""
        if not self.webhook_config.get('enabled', False):
            return False

        try:
            webhook_url = self.webhook_config.get('url')
            headers = self.webhook_config.get('headers', {'Content-Type': 'application/json'})

            response = requests.post(
                webhook_url,
                headers=headers,
                json=payload,
                timeout=10
            )

            if response.status_code >= 200 and response.status_code < 300:
                logger.info(f"WebHook通知发送成功")
                return True
            else:
                logger.error(f"WebHook通知发送失败: HTTP {response.status_code}, {response.text}")
                return False

        except Exception as e:
            logger.error(f"发送WebHook通知失败: {e}")
            return False

    def _send_slack(self, blocks):
        """发送Slack通知"""
        if not self.slack_config.get('enabled', False):
            return False

        try:
            webhook_url = self.slack_config.get('webhook_url')

            payload = {
                "blocks": blocks
            }

            response = requests.post(
                webhook_url,
                json=payload,
                timeout=10
            )

            if response.status_code == 200:
                logger.info(f"Slack通知发送成功")
                return True
            else:
                logger.error(f"Slack通知发送失败: HTTP {response.status_code}, {response.text}")
                return False

        except Exception as e:
            logger.error(f"发送Slack通知失败: {e}")
            return False

    def _send_discord(self, embed):
        """发送Discord通知"""
        if not self.discord_config.get('enabled', False):
            return False

        try:
            webhook_url = self.discord_config.get('webhook_url')

            payload = {
                "embeds": [embed]
            }

            response = requests.post(
                webhook_url,
                json=payload,
                timeout=10
            )

            if response.status_code == 204:
                logger.info(f"Discord通知发送成功")
                return True
            else:
                logger.error(f"Discord通知发送失败: HTTP {response.status_code}, {response.text}")
                return False

        except Exception as e:
            logger.error(f"发送Discord通知失败: {e}")
            return False

    def _send_wechat(self, message):
        """发送企业微信通知"""
        if not self.wechat_config.get('enabled', False):
            return False

        try:
            webhook_url = self.wechat_config.get('webhook_url')

            response = requests.post(
                webhook_url,
                json=message,
                timeout=10
            )

            if response.status_code == 200 and response.json().get('errcode') == 0:
                logger.info(f"企业微信通知发送成功")
                return True
            else:
                logger.error(f"企业微信通知发送失败: {response.text}")
                return False

        except Exception as e:
            logger.error(f"发送企业微信通知失败: {e}")
            return False

    def notify_new_papers(self, papers):
        """通知新论文"""
        if not papers:
            return

        if not self._should_notify('papers', len(papers)):
            return

        try:
            # 邮件通知
            if self.email_config.get('enabled', False):
                subject = f"NeuroCrawler: 发现{len(papers)}篇新论文"

                html_content = f"""
                <h2>NeuroCrawler 爬取报告</h2>
                <p>爬取时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p>发现 <strong>{len(papers)}</strong> 篇新论文</p>
                <h3>论文列表:</h3>
                <ul>
                """

                # 最多显示10篇论文
                for i, paper in enumerate(papers[:10]):
                    html_content += f"""
                    <li>
                        <strong>{paper.get('title', '无标题')}</strong><br>
                        <span>来源: {paper.get('source', '未知')} - {paper.get('journal', '未知期刊')}</span><br>
                        <span>发布日期: {paper.get('published_date').strftime('%Y-%m-%d') if paper.get('published_date') else '未知'}</span><br>
                        <a href="{paper.get('url', '#')}">查看论文</a>
                    </li>
                    <br>
                    """

                if len(papers) > 10:
                    html_content += f"<li>... 以及其他 {len(papers) - 10} 篇论文</li>"

                html_content += "</ul>"

                if any('datasets' in paper and paper['datasets'] for paper in papers):
                    html_content += "<p>有些论文包含可用数据集，请查看数据集通知或系统日志获取详情。</p>"

                self._send_email(subject, html_content)

            # 钉钉通知
            if self.dingtalk_config.get('enabled', False):
                paper_list = ""
                for i, paper in enumerate(papers[:5], 1):
                    title = paper.get('title', '无标题')
                    url = paper.get('url', '#')
                    source = paper.get('source', '未知')
                    journal = paper.get('journal', '未知期刊')
                    paper_list += f"{i}. [{title}]({url}) - {source}/{journal}\n"

                if len(papers) > 5:
                    paper_list += f"... 以及其他 {len(papers) - 5} 篇论文"

                message = {
                    "msgtype": "markdown",
                    "markdown": {
                        "title": f"NeuroCrawler: 发现{len(papers)}篇新论文",
                        "text": f"""
# NeuroCrawler 爬取报告
爬取时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 发现 {len(papers)} 篇新论文

{paper_list}
                        """
                    }
                }

                self._send_dingtalk(message)

            # WebHook通知
            if self.webhook_config.get('enabled', False):
                payload = {
                    "event": "new_papers",
                    "timestamp": datetime.now().isoformat(),
                    "count": len(papers),
                    "papers": []
                }

                # 添加论文数据
                for paper in papers[:20]:  # 限制数量避免过大
                    paper_data = {
                        "title": paper.get('title', '无标题'),
                        "url": paper.get('url', ''),
                        "source": paper.get('source', '未知'),
                        "journal": paper.get('journal', '未知期刊'),
                        "published_date": paper.get('published_date').isoformat() if paper.get(
                            'published_date') else None,
                        "doi": paper.get('doi', ''),
                        "has_datasets": 'datasets' in paper and bool(paper['datasets'])
                    }
                    payload["papers"].append(paper_data)

                self._send_webhook(payload)

        except Exception as e:
            logger.error(f"发送新论文通知时出错: {e}")

    def notify_new_datasets(self, datasets):
        """通知新数据集"""
        if not datasets:
            return

        if not self._should_notify('datasets', len(datasets)):
            return

        try:
            # 邮件通知
            if self.email_config.get('enabled', False):
                subject = f"NeuroCrawler: 发现{len(datasets)}个新数据集"

                html_content = f"""
                <h2>NeuroCrawler 数据集通知</h2>
                <p>发现时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p>发现 <strong>{len(datasets)}</strong> 个新数据集</p>
                <h3>数据集列表:</h3>
                <ul>
                """

                # 分类统计
                data_types_count = {}
                for dataset in datasets:
                    types = dataset.get('data_types', [])
                    if not types:
                        types = ['unclassified']

                    for data_type in types:
                        data_types_count[data_type] = data_types_count.get(data_type, 0) + 1

                html_content += "<h4>数据类型统计:</h4><ul>"
                for data_type, count in data_types_count.items():
                    html_content += f"<li>{data_type}: {count}个</li>"
                html_content += "</ul>"

                # 最多显示10个数据集
                for i, dataset in enumerate(datasets[:10]):
                    dataset_types = ", ".join(dataset.get('data_types', ['未分类']))
                    html_content += f"""
                    <li>
                        <strong>{dataset.get('name', '未命名数据集')}</strong><br>
                        <span>类型: {dataset_types}</span><br>
                        <span>仓库: {dataset.get('repository', '未知')}</span><br>
                        <a href="{dataset.get('url', '#')}">访问数据集</a>
                    </li>
                    <br>
                    """

                if len(datasets) > 10:
                    html_content += f"<li>... 以及其他 {len(datasets) - 10} 个数据集</li>"

                html_content += "</ul>"
                html_content += "<p>您可以使用NeuroCrawler的下载功能获取这些数据集。</p>"

                self._send_email(subject, html_content)

            # 钉钉通知
            if self.dingtalk_config.get('enabled', False):
                data_type_stats = ""
                data_types_count = {}
                for dataset in datasets:
                    types = dataset.get('data_types', [])
                    if not types:
                        types = ['unclassified']

                    for data_type in types:
                        data_types_count[data_type] = data_types_count.get(data_type, 0) + 1

                for data_type, count in data_types_count.items():
                    data_type_stats += f"- {data_type}: {count}个\n"

                dataset_list = ""
                for i, dataset in enumerate(datasets[:5], 1):
                    name = dataset.get('name', '未命名数据集')
                    url = dataset.get('url', '#')
                    repo = dataset.get('repository', '未知')
                    dataset_list += f"{i}. [{name}]({url}) - {repo}\n"

                if len(datasets) > 5:
                    dataset_list += f"... 以及其他 {len(datasets) - 5} 个数据集"

                message = {
                    "msgtype": "markdown",
                    "markdown": {
                        "title": f"NeuroCrawler: 发现{len(datasets)}个新数据集",
                        "text": f"""
# NeuroCrawler 数据集通知
发现时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 发现 {len(datasets)} 个新数据集

### 数据类型统计:
{data_type_stats}

### 数据集列表:
{dataset_list}
                        """
                    }
                }

                self._send_dingtalk(message)

            # WebHook通知
            if self.webhook_config.get('enabled', False):
                payload = {
                    "event": "new_datasets",
                    "timestamp": datetime.now().isoformat(),
                    "count": len(datasets),
                    "datasets": []
                }

                # 添加数据集数据
                for dataset in datasets[:20]:  # 限制数量避免过大
                    dataset_data = {
                        "name": dataset.get('name', '未命名数据集'),
                        "url": dataset.get('url', ''),
                        "repository": dataset.get('repository', '未知'),
                        "data_types": dataset.get('data_types', []),
                        "doi": dataset.get('doi', '')
                    }
                    payload["datasets"].append(dataset_data)

                self._send_webhook(payload)

        except Exception as e:
            logger.error(f"发送新数据集通知时出错: {e}")

    def notify_new_repositories(self, repositories):
        """通知新GitHub仓库"""
        if not repositories:
            return

        if not self._should_notify('repositories', len(repositories)):
            return

        try:
            # 邮件通知
            if self.email_config.get('enabled', False):
                subject = f"NeuroCrawler: 发现{len(repositories)}个新GitHub仓库"

                html_content = f"""
                <h2>NeuroCrawler GitHub仓库通知</h2>
                <p>发现时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p>发现 <strong>{len(repositories)}</strong> 个新GitHub仓库</p>
                <h3>仓库列表:</h3>
                <ul>
                """

                # 语言统计
                language_count = {}
                for repo in repositories:
                    lang = repo.get('language', 'unknown')
                    language_count[lang] = language_count.get(lang, 0) + 1

                html_content += "<h4>语言统计:</h4><ul>"
                for lang, count in language_count.items():
                    html_content += f"<li>{lang}: {count}个</li>"
                html_content += "</ul>"

                # 最多显示10个仓库
                for i, repo in enumerate(repositories[:10]):
                    stars = repo.get('stars', 0)
                    forks = repo.get('forks', 0)
                    html_content += f"""
                    <li>
                        <strong>{repo.get('full_name', '未知仓库')}</strong><br>
                        <span>描述: {repo.get('description', '无描述')}</span><br>
                        <span>语言: {repo.get('language', '未知')} | ⭐ {stars} | 🍴 {forks}</span><br>
                        <a href="{repo.get('url', '#')}">访问仓库</a>
                    </li>
                    <br>
                    """

                if len(repositories) > 10:
                    html_content += f"<li>... 以及其他 {len(repositories) - 10} 个仓库</li>"

                html_content += "</ul>"

                self._send_email(subject, html_content)

            # 钉钉通知
            if self.dingtalk_config.get('enabled', False):
                language_stats = ""
                language_count = {}
                for repo in repositories:
                    lang = repo.get('language', 'unknown')
                    language_count[lang] = language_count.get(lang, 0) + 1

                for lang, count in language_count.items():
                    language_stats += f"- {lang}: {count}个\n"

                repo_list = ""
                for i, repo in enumerate(repositories[:5], 1):
                    name = repo.get('full_name', '未知仓库')
                    url = repo.get('url', '#')
                    stars = repo.get('stars', 0)
                    repo_list += f"{i}. [{name}]({url}) - ⭐ {stars}\n"

                if len(repositories) > 5:
                    repo_list += f"... 以及其他 {len(repositories) - 5} 个仓库"

                message = {
                    "msgtype": "markdown",
                    "markdown": {
                        "title": f"NeuroCrawler: 发现{len(repositories)}个新GitHub仓库",
                        "text": f"""
# NeuroCrawler GitHub仓库通知
发现时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 发现 {len(repositories)} 个新GitHub仓库

### 语言统计:
{language_stats}

### 仓库列表:
{repo_list}
                        """
                    }
                }

                self._send_dingtalk(message)

            # WebHook通知
            if self.webhook_config.get('enabled', False):
                payload = {
                    "event": "new_repositories",
                    "timestamp": datetime.now().isoformat(),
                    "count": len(repositories),
                    "repositories": []
                }

                # 添加仓库数据
                for repo in repositories[:20]:  # 限制数量避免过大
                    repo_data = {
                        "full_name": repo.get('full_name', ''),
                        "url": repo.get('url', ''),
                        "description": repo.get('description', ''),
                        "language": repo.get('language', ''),
                        "stars": repo.get('stars', 0),
                        "forks": repo.get('forks', 0)
                    }
                    payload["repositories"].append(repo_data)

                self._send_webhook(payload)

        except Exception as e:
            logger.error(f"发送新GitHub仓库通知时出错: {e}")

    def notify_download_results(self, download_results):
        """通知数据集下载结果"""
        if self.disabled:
            return

        try:
            total = download_results.get('total', 0)
            success = download_results.get('success', 0)
            failed = download_results.get('failed', 0)
            skipped = download_results.get('skipped', 0)

            if total == 0:
                return

            # 邮件通知
            if self.email_config.get('enabled', False):
                subject = f"NeuroCrawler数据集下载报告: {success}个成功, {failed}个失败"

                html_content = f"""
                <h2>NeuroCrawler数据集下载报告</h2>
                <p>下载时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

                <h3>下载统计</h3>
                <ul>
                    <li>总计数据集: {total}</li>
                    <li>成功下载: {success}</li>
                    <li>下载失败: {failed}</li>
                    <li>已存在跳过: {skipped}</li>
                </ul>
                """

                # 添加下载详情
                if 'details' in download_results and download_results['details']:
                    html_content += "<h3>下载详情</h3><ul>"

                    for detail in download_results['details'][:10]:  # 只显示前10个
                        status = "成功" if detail.get('success') else "失败"
                        name = detail.get('dataset', 'Unknown')
                        repo = detail.get('repository', 'Unknown')
                        error = detail.get('error', '')

                        html_content += f"<li>{name} ({repo}): {status}"
                        if error:
                            html_content += f" - 错误: {error}"
                        html_content += "</li>"

                    if len(download_results['details']) > 10:
                        html_content += f"<li>... 以及其他 {len(download_results['details']) - 10} 个数据集</li>"

                    html_content += "</ul>"

                self._send_email(subject, html_content)

            # 钉钉通知
            if self.dingtalk_config.get('enabled', False):
                # 构建钉钉消息
                message = {
                    "msgtype": "markdown",
                    "markdown": {
                        "title": f"NeuroCrawler数据集下载报告",
                        "text": f"""
                        # NeuroCrawler数据集下载报告
                        下载时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

                        ### 下载统计
                        - 总计数据集: {total}
                        - 成功下载: {success}
                        - 下载失败: {failed}
                        - 已存在跳过: {skipped}
                        """
                    }
                }

                # 如果有详细信息，添加部分详情
                if 'details' in download_results and download_results['details']:
                    details_text = "\n### 部分详情\n"
                    for i, detail in enumerate(download_results['details'][:5]):
                        status = "✅" if detail.get('success') else "❌"
                        name = detail.get('dataset', 'Unknown')
                        details_text += f"{i + 1}. {status} {name}\n"

                    if len(download_results['details']) > 5:
                        details_text += f"... 以及其他 {len(download_results['details']) - 5} 个数据集"

                    message["markdown"]["text"] += details_text

                self._send_dingtalk(message)

            # WebHook通知
            if self.webhook_config.get('enabled', False):
                payload = {
                    "event": "download_results",
                    "timestamp": datetime.now().isoformat(),
                    "statistics": {
                        "total": total,
                        "success": success,
                        "failed": failed,
                        "skipped": skipped
                    }
                }

                # 添加部分详情
                if 'details' in download_results and download_results['details']:
                    payload["details"] = download_results['details'][:20]  # 限制数量

                self._send_webhook(payload)

        except Exception as e:
            logger.error(f"发送下载结果通知时出错: {e}")