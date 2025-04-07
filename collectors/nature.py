#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import re
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from collectors.base_collector import BaseCollector
from utils.selectors import NATURE_SELECTORS

logger = logging.getLogger(__name__)


class NatureCollector(BaseCollector):
    """
    用于从Nature及其子刊爬取神经科学相关论文和数据集的爬虫
    """

    def __init__(self, config):
        super().__init__(config)

        # 期刊信息
        self.journals = {
            'nature': {
                'name': 'Nature',
                'base_url': 'https://www.nature.com/nature',
                'advanced_search_url': 'https://www.nature.com/search',
                'articles_api': 'https://www.nature.com/nature/articles',
            },
            'nature-neuroscience': {
                'name': 'Nature Neuroscience',
                'base_url': 'https://www.nature.com/neuro',
                'advanced_search_url': 'https://www.nature.com/search',
                'articles_api': 'https://www.nature.com/neuro/articles',
            },
            'nature-methods': {
                'name': 'Nature Methods',
                'base_url': 'https://www.nature.com/nmeth',
                'advanced_search_url': 'https://www.nature.com/search',
                'articles_api': 'https://www.nature.com/nmeth/articles',
            },
            'nature-communications': {
                'name': 'Nature Communications',
                'base_url': 'https://www.nature.com/ncomms',
                'advanced_search_url': 'https://www.nature.com/search',
                'articles_api': 'https://www.nature.com/ncomms/articles',
            },
            'scientific-reports': {
                'name': 'Scientific Reports',
                'base_url': 'https://www.nature.com/srep',
                'advanced_search_url': 'https://www.nature.com/search',
                'articles_api': 'https://www.nature.com/srep/articles',
            },
            'nature-machine-intelligence': {
                'name': 'Nature Machine Intelligence',
                'base_url': 'https://www.nature.com/natmachintell',
                'advanced_search_url': 'https://www.nature.com/search',
                'articles_api': 'https://www.nature.com/natmachintell/articles',
            },
            'nature-biotechnology': {
                'name': 'Nature Biotechnology',
                'base_url': 'https://www.nature.com/nbt',
                'advanced_search_url': 'https://www.nature.com/search',
                'articles_api': 'https://www.nature.com/nbt/articles',
            }
        }

        # 神经科学关键词
        self.neuroscience_keywords = [
            "neuroscience", "neural", "brain", "neuron", "cortex",
            "cognition", "cognitive", "neuroimaging", "connectome",
            "neuroinformatics", "computational neuroscience"
        ]

    def search_articles(self, start_date, end_date, **kwargs):
        """搜索符合条件的文章"""
        all_articles = []

        # 获取配置的期刊列表
        journals_config = self.config.get('journals', list(self.journals.keys()))

        # 如果journals_config是字典，转换为列表
        if isinstance(journals_config, dict):
            enabled_journals = []
            for journal_id, journal_config in journals_config.items():
                if journal_config.get('enabled', True):
                    enabled_journals.append(journal_id)
            journals_config = enabled_journals

        # 确保journals_config是列表
        if not isinstance(journals_config, list):
            journals_config = list(self.journals.keys())

        # 遍历期刊列表
        for journal_id in journals_config:
            if journal_id not in self.journals:
                logger.warning(f"未知的期刊ID: {journal_id}")
                continue

            journal_info = self.journals[journal_id]
            logger.info(f"正在搜索期刊: {journal_info['name']}")

            try:
                # 搜索文章
                articles = self._search_journal_articles(
                    journal_id,
                    start_date,
                    end_date
                )

                logger.info(f"从 {journal_info['name']} 搜索到 {len(articles)} 篇文章")
                all_articles.extend(articles)

                # 间隔一段时间再搜索下一个期刊
                self._random_delay(3, 6)

            except Exception as e:
                logger.error(f"搜索期刊 {journal_info['name']} 时出错: {e}")

        logger.info(f"从所有期刊共搜索到 {len(all_articles)} 篇文章")
        return all_articles

    def _search_journal_articles(self, journal_id, start_date, end_date, page=1, page_size=100):
        """搜索单个期刊的文章"""
        journal_info = self.journals.get(journal_id)
        if not journal_info:
            logger.error(f"未知的期刊ID: {journal_id}")
            return []

        try:
            # 构建查询参数
            params = {
                'journal': journal_id,
                'date_range': f'{self._format_date(start_date)} TO {self._format_date(end_date)}',
                'order': 'date_desc',
                'page': page,
                'page_size': page_size,
                'nature_research': 'yes'
            }

            # 使用OR连接神经科学关键词 - 这行必须添加
            neuro_search_terms = " OR ".join([f'"{keyword}"' for keyword in self.neuroscience_keywords])
            params['q'] = f'({neuro_search_terms})'

            # 构建完整的搜索URL
            # 为了防止URL编码问题，使用urllib.parse的urlencode函数
            from urllib.parse import urlencode
            search_url = f"{journal_info['advanced_search_url']}?{urlencode(params)}"

            logger.info(f"搜索URL: {search_url}")

            # 使用浏览器模拟器获取页面
            html_content = self.browser.get_page(
                search_url,
                use_selenium=self.config.get('browser_emulation', True),
                wait_time=15
            )

            if not html_content:
                logger.error(f"获取搜索页面失败: {search_url}")
                return []

            # 缓存HTML，便于调试
            self.save_html_cache(search_url, html_content)

            # 解析搜索结果
            return self._parse_search_results(html_content, journal_info)

        except Exception as e:
            logger.error(f"搜索期刊文章时出错: {e}")
            return []

    def _parse_search_results(self, html_content, journal_info):
        """解析搜索结果页面"""
        articles = []

        try:
            # 保存HTML用于检查实际结构
            import os
            from datetime import datetime
            debug_dir = "debug_html"
            os.makedirs(debug_dir, exist_ok=True)
            debug_file = os.path.join(debug_dir,
                                      f"{journal_info['name'].replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html")
            with open(debug_file, "w", encoding="utf-8") as f:
                f.write(html_content)
            logger.info(f"已保存HTML用于调试: {debug_file}")

            soup = BeautifulSoup(html_content, 'html.parser')

            # 检查是否显示"无结果"消息
            no_results_indicators = [
                'No results found',
                'Sorry, there are no results',
                '没有找到结果',
                '0 results found',
                'Your search did not match any articles'
            ]

            page_text = soup.get_text().lower()
            for indicator in no_results_indicators:
                if indicator.lower() in page_text:
                    logger.warning(f"搜索返回无结果: '{indicator}' 在页面中找到")
                    return []

            # 尝试所有可能的文章项选择器
            article_elements = []
            used_selector = None

            # 可能的文章元素选择器
            article_selectors = [
                'li.app-article-list-row',
                'li.c-list-group__item',
                'article.u-full-width',
                'div.c-card.c-card--flush',
                'div.u-flex-justify-between',
                'li.has-journal-link',
                'article[data-track-action="view article"]',
                'ul.c-list-group > li',  # 通用列表项
                'div.app-article-list-row',  # 另一个可能的容器
                'div[data-component="article-list"] > div',  # 更通用的选择器
                '.c-card',  # 简单的卡片类
                '.article-item'  # 通用文章项类
            ]

            for selector in article_selectors:
                elements = soup.select(selector)
                if elements:
                    article_elements = elements
                    used_selector = selector
                    logger.info(f"使用选择器 '{selector}' 找到 {len(elements)} 个文章元素")
                    break

            # 如果找不到文章，尝试更通用的方法
            if not article_elements:
                logger.warning("无法使用预定义选择器找到文章，尝试替代方法")

                # 尝试查找包含特定模式的链接
                all_links = soup.find_all('a')
                article_links = []

                for link in all_links:
                    href = link.get('href', '')
                    # 检查链接是否指向文章
                    article_patterns = ['/articles/', '/article/', '/s', 'doi.org']
                    if any(pattern in href for pattern in article_patterns):
                        parent = link.parent
                        # 如果链接位于标题中或是主要内容链接，可能是文章链接
                        if parent and parent.name in ['h1', 'h2', 'h3', 'h4', 'h5'] or 'c-card__link' in link.get(
                                'class', []):
                            article_links.append(link)

                if article_links:
                    logger.info(f"通过链接模式找到 {len(article_links)} 个可能的文章")

                    # 从链接创建文章
                    for link in article_links:
                        # 尝试提取标题
                        title = link.text.strip()

                        # 如果链接本身没有文本，查找相关元素
                        if not title:
                            # 检查父元素是否是标题
                            parent = link.parent
                            if parent and parent.name in ['h1', 'h2', 'h3', 'h4', 'h5']:
                                title = parent.text.strip()
                            else:
                                # 查找链接内部或相邻的标题元素
                                title_el = link.find(['h1', 'h2', 'h3', 'h4', 'h5'])
                                if not title_el:
                                    title_el = link.find_next(['h1', 'h2', 'h3', 'h4', 'h5'])
                                if title_el:
                                    title = title_el.text.strip()

                        # 如果仍然没有找到标题，跳过
                        if not title:
                            continue

                        article_url = urljoin(journal_info['base_url'], link['href'])

                        # 创建文章对象
                        article = {
                            'title': title,
                            'url': article_url,
                            'journal': journal_info['name'],
                            'source': 'nature'
                        }

                        # 尝试提取发布日期
                        date_el = None
                        # 查找附近的时间元素
                        date_el = link.find_next('time')
                        if not date_el:
                            # 向上搜索共同的父元素，然后查找time元素
                            parent = link.parent
                            while parent and parent.name != 'body':
                                date_el = parent.find('time')
                                if date_el:
                                    break
                                parent = parent.parent

                        if date_el:
                            pub_date = None
                            if date_el.get('datetime'):
                                try:
                                    pub_date = datetime.strptime(date_el['datetime'], "%Y-%m-%d")
                                    article['published_date'] = pub_date.strftime("%Y-%m-%d")
                                except ValueError:
                                    pass
                            if not pub_date and date_el.text.strip():
                                try:
                                    # 尝试解析各种日期格式
                                    date_text = date_el.text.strip()
                                    for fmt in ["%d %b %Y", "%B %d, %Y", "%Y-%m-%d"]:
                                        try:
                                            pub_date = datetime.strptime(date_text, fmt)
                                            article['published_date'] = pub_date.strftime("%Y-%m-%d")
                                            break
                                        except ValueError:
                                            continue
                                except:
                                    pass

                        # 尝试提取作者
                        authors = []
                        # 查找常见的作者容器模式
                        author_containers = link.find_all(['span', 'div'], class_=lambda c: c and (
                                    'author' in c.lower() or 'c-author-list' in c))
                        for container in author_containers:
                            author_elements = container.find_all('li')
                            if author_elements:
                                authors = [author.text.strip() for author in author_elements]
                                break

                        if authors:
                            article['authors'] = authors

                        articles.append(article)

                # 如果替代方法也没找到文章，返回空列表
                return articles

            # 处理找到的文章元素
            for article_el in article_elements:
                try:
                    # 尝试多个可能的标题选择器
                    title_el = None
                    title_text = None

                    title_selectors = [
                        'h3.c-card__title a',
                        'a.c-card__link',
                        'h2 a',
                        'h3 a',
                        'a[data-track-action="view article"]',
                        'a.u-link-inherit',
                        '.c-card__title a',
                        'h1 a', 'h2 a', 'h3 a', 'h4 a',
                        'a'  # 最后尝试任何链接
                    ]

                    for title_selector in title_selectors:
                        title_els = article_el.select(title_selector)
                        for el in title_els:
                            if el.text.strip():  # 确保有文本内容
                                title_el = el
                                title_text = el.text.strip()
                                break
                        if title_text:
                            break

                    # 如果没有找到标题，查找任何可能的标题
                    if not title_text:
                        heading_tags = article_el.find_all(['h1', 'h2', 'h3', 'h4', 'h5'])
                        for heading in heading_tags:
                            if heading.text.strip():
                                title_text = heading.text.strip()
                                # 查找标题中的链接
                                title_el = heading.find('a')
                                break

                    # 如果仍然没有找到标题，尝试查找卡片标题或任何有意义的文本
                    if not title_text:
                        card_titles = article_el.select('.c-card__title, .article-title, .title')
                        for card_title in card_titles:
                            title_text = card_title.text.strip()
                            if title_text:
                                break

                    # 如果没有找到任何标题，跳过这个文章
                    if not title_text:
                        continue

                    # 获取文章URL
                    article_url = None
                    if title_el and 'href' in title_el.attrs:
                        article_url = urljoin(journal_info['base_url'], title_el['href'])
                    else:
                        # 尝试在整个文章元素中找链接
                        link_els = article_el.find_all('a')
                        for link_el in link_els:
                            if 'href' in link_el.attrs:
                                href = link_el['href']
                                # 检查是否是文章链接
                                if '/article/' in href or '/articles/' in href or 'doi.org' in href:
                                    article_url = urljoin(journal_info['base_url'], href)
                                    break

                    if not article_url:
                        continue

                    # 创建文章对象
                    article = {
                        'title': title_text,
                        'url': article_url,
                        'journal': journal_info['name'],
                        'source': 'nature'
                    }

                    # 提取发布日期
                    date_el = article_el.select_one('time')
                    if date_el:
                        pub_date = None
                        if date_el.get('datetime'):
                            try:
                                pub_date = datetime.strptime(date_el['datetime'], "%Y-%m-%d")
                                article['published_date'] = pub_date.strftime("%Y-%m-%d")
                            except ValueError:
                                try:
                                    date_text = date_el.text.strip()
                                    for fmt in ["%d %b %Y", "%B %d, %Y", "%Y-%m-%d"]:
                                        try:
                                            pub_date = datetime.strptime(date_text, fmt)
                                            article['published_date'] = pub_date.strftime("%Y-%m-%d")
                                            break
                                        except ValueError:
                                            continue
                                except:
                                    pass

                    # 提取作者
                    authors_els = article_el.select('ul.c-author-list li, .c-author-list__item, .authors span')
                    if authors_els:
                        article['authors'] = [author.text.strip() for author in authors_els]

                    articles.append(article)

                except Exception as e:
                    logger.error(f"解析文章元素时出错: {e}")
                    import traceback
                    logger.debug(f"详细错误: {traceback.format_exc()}")

            logger.info(f"从 {journal_info['name']} 搜索到 {len(articles)} 篇文章")
            return articles

        except Exception as e:
            logger.error(f"解析搜索结果页面时出错: {e}")
            import traceback
            logger.debug(f"详细错误: {traceback.format_exc()}")
            return []

    def get_article_details(self, article):
        """获取文章详细信息"""
        if not article or 'url' not in article:
            return article

        # 获取文章URL
        article_url = article['url']

        try:
            # 使用浏览器模拟器获取页面
            html_content = self.browser.get_page(
                article_url,
                use_selenium=self.config.get('browser_emulation', True),
                wait_time=15
            )

            if not html_content:
                logger.error(f"获取文章详情失败: {article_url}")
                return article

            # 缓存HTML内容
            self.save_html_cache(article_url, html_content)

            # 保存HTML内容，供提取数据集使用
            article['html_content'] = html_content

            # 解析页面
            soup = BeautifulSoup(html_content, 'html.parser')

            # 提取DOI
            for selector in NATURE_SELECTORS['doi']:
                doi_el = soup.select_one(selector)
                if doi_el:
                    if doi_el.name == 'meta':
                        article['doi'] = doi_el.get('content')
                    else:
                        article['doi'] = doi_el.text.strip()
                    break

            # 提取摘要
            for selector in NATURE_SELECTORS['abstract']:
                abstract_el = soup.select_one(selector)
                if abstract_el:
                    if abstract_el.name == 'meta':
                        article['abstract'] = abstract_el.get('content')
                    else:
                        article['abstract'] = abstract_el.text.strip()
                    break

            # 提取PDF链接
            for selector in NATURE_SELECTORS['pdf_link']:
                pdf_el = soup.select_one(selector)
                if pdf_el and 'href' in pdf_el.attrs:
                    article['pdf_url'] = urljoin(article_url, pdf_el['href'])
                    break

            # 提取补充材料链接
            for selector in NATURE_SELECTORS['supplementary']:
                supp_el = soup.select_one(selector)
                if supp_el and 'href' in supp_el.attrs:
                    article['supplementary_url'] = urljoin(article_url, supp_el['href'])
                    break

            # 提取关键词
            keywords_el = soup.select('meta[name="keywords"], meta[property="article:tag"]')
            if keywords_el:
                keywords = []
                for el in keywords_el:
                    content = el.get('content')
                    if content:
                        keywords.extend([k.strip() for k in content.split(',')])
                article['keywords'] = list(set(keywords))

            return article

        except Exception as e:
            logger.error(f"获取文章详情时出错: {e}, url: {article_url}")
            return article