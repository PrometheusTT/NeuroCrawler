#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import re
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlencode, urljoin
from collectors.base_collector import BaseCollector
from utils.selectors import NATURE_SELECTORS
import os
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

        # 遍历期刊列表
        for journal_id in journals_config:
            if journal_id not in self.journals:
                logger.warning(f"未知的期刊ID: {journal_id}")
                continue

            journal_info = self.journals[journal_id]
            logger.info(f"正在搜索期刊: {journal_info['name']}")

            try:
                # 尝试搜索文章
                articles = self._search_journal_articles(
                    journal_id,
                    start_date,
                    end_date
                )

                # 如果没有找到文章，尝试直接获取
                if not articles and journal_id != 'nature':  # 主刊通常可以正常搜索
                    logger.info(f"搜索没有找到文章，尝试直接从{journal_info['name']}获取最新文章")
                    articles = self._get_latest_articles_direct(journal_id)

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
            # 改用更可靠的方式构建URL
            from urllib.parse import urlencode  # 使用正确的模块

            # 构建查询参数 - 确保所有值都是字符串类型
            params = {
                'journal': str(journal_id),
                'date_range': f'{self._format_date(start_date)} TO {self._format_date(end_date)}',
                'order': 'date_desc',
                'page': str(page),
                'page_size': str(page_size),
                'nature_research': 'yes'
            }

            # 使用OR连接神经科学关键词
            neuro_search_terms = " OR ".join([f'"{keyword}"' for keyword in self.neuroscience_keywords])
            params['q'] = f'({neuro_search_terms})'

            # 直接使用urllib.parse的urlencode方法构建URL
            query_string = urlencode(params)  # 使用正确的函数
            search_url = f"{journal_info['advanced_search_url']}?{query_string}"

            logger.info(f"完整搜索URL: {search_url}")

            # 避免URL截断问题的验证
            if len(search_url) > 200:
                logger.info(f"URL长度: {len(search_url)}，前200字符: {search_url[:200]}...")

            # 使用浏览器模拟器获取页面
            html_content = self.browser.get_page(
                search_url,
                use_selenium=self.config.get('browser_emulation', True),
                wait_time=20  # 增加等待时间
            )

            if not html_content:
                logger.error(f"获取搜索页面失败: {search_url}")
                return []

            # 验证获取到的页面长度
            logger.info(f"获取到HTML长度: {len(html_content)}")

            # 保存HTML，便于调试
            self.save_html_cache(search_url, html_content)

            # 解析搜索结果
            return self._parse_search_results(html_content, journal_info, params)

        except Exception as e:
            logger.error(f"搜索期刊文章时出错: {e}")
            import traceback
            logger.error(traceback.format_exc())  # 添加完整堆栈跟踪
            return []

    def _parse_search_results(self, html_content, journal_info, params=None):
        """解析搜索结果页面"""
        articles = []

        try:
            # 现有的保存HTML代码...

            soup = BeautifulSoup(html_content, 'html.parser')

            # 增加HTML分析功能，帮助理解页面结构
            page_info = {
                'title': soup.title.text if soup.title else 'No title',
                'meta_description': None
            }

            # 检查meta描述
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc:
                page_info['meta_description'] = meta_desc.get('content')

            logger.info(f"页面标题: {page_info['title']}")

            # 检查页面是否包含无结果信息
            page_text = soup.get_text().lower()
            no_results_phrases = [
                'no results found',
                'sorry, there are no results',
                'your search did not match',
                '0 results found',
                'we couldn\'t find'
            ]

            for phrase in no_results_phrases:
                if phrase in page_text:
                    logger.warning(f"检测到无结果提示: '{phrase}'")
                    # 找到显示这个提示的元素
                    elements = soup.find_all(string=lambda text: phrase in text.lower() if text else False)
                    if elements:
                        parent = elements[0].parent
                        logger.info(f"无结果提示元素: {parent.name} - {parent.get('class', 'no-class')}")
                    return []

            # 检查所有可能包含文章列表的容器
            potential_containers = [
                'main', '#content', '.content-wrapper', '.content-container',
                '.c-list-group', '.app-article-list', '.search-results',
                'div[data-component="search-results"]', 'div[data-test="search-results"]'
            ]

            container_info = []
            for selector in potential_containers:
                elements = soup.select(selector)
                for i, element in enumerate(elements):
                    class_attr = element.get('class', [])
                    class_str = ' '.join(class_attr) if class_attr else 'no-class'
                    id_attr = element.get('id', 'no-id')

                    # 检查该容器内是否有潜在的文章元素
                    potential_articles = element.select('article, .c-card, li, div > a[href*="/articles/"]')

                    container_info.append({
                        'selector': selector,
                        'index': i,
                        'id': id_attr,
                        'class': class_str,
                        'potential_articles': len(potential_articles)
                    })

            if container_info:
                logger.info(f"找到 {len(container_info)} 个潜在内容容器:")
                for info in container_info:
                    if info['potential_articles'] > 0:
                        logger.info(
                            f"  - {info['selector']} #{info['index']}: ID={info['id']}, Class={info['class']}, 包含 {info['potential_articles']} 个潜在文章")

                        # 尝试从这个容器解析文章
                        container = soup.select(info['selector'])[info['index']]

                        # 这里增加特定于容器的文章提取逻辑
                        articles.extend(self._extract_articles_from_container(container, journal_info))

            # 如果通过容器没有找到文章，尝试直接搜索文章链接
            if not articles:
                logger.info("尝试直接搜索文章链接")
                all_links = soup.find_all('a')
                article_links = []

                for link in all_links:
                    href = link.get('href', '')
                    if '/articles/' in href or '/article/' in href:
                        title = link.text.strip()
                        if title:
                            article_url = urljoin(journal_info['base_url'], href)
                            article = {
                                'title': title,
                                'url': article_url,
                                'journal': journal_info['name'],
                                'source': 'nature'
                            }
                            articles.append(article)
                            logger.info(f"直接找到文章: {title}")

            logger.info(f"从 {journal_info['name']} 搜索到 {len(articles)} 篇文章")
            return articles

        except Exception as e:
            logger.error(f"解析搜索结果页面时出错: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []

    def _extract_articles_from_container(self, container, journal_info):
        """从找到的容器中提取文章"""
        articles = []

        # 1. 尝试找符合Nature结构的文章
        article_elements = container.select('.c-card, article, li.app-article-list-row')

        # 2. 如果没找到，尝试所有a标签
        if not article_elements:
            article_elements = container.select('a[href*="/articles/"], a[href*="/article/"]')

        # 3. 处理找到的元素
        for element in article_elements:
            try:
                # 如果是链接元素
                if element.name == 'a':
                    title = element.text.strip()
                    url = urljoin(journal_info['base_url'], element['href'])

                    if not title:
                        # 查找元素内的标题
                        title_el = element.select_one('h1, h2, h3, h4, h5, .title')
                        if title_el:
                            title = title_el.text.strip()

                    if title:
                        articles.append({
                            'title': title,
                            'url': url,
                            'journal': journal_info['name'],
                            'source': 'nature'
                        })
                else:
                    # 如果是卡片或文章元素
                    title_el = element.select_one('h1, h2, h3, h4, h5, .title, a')
                    if not title_el:
                        continue

                    title = title_el.text.strip()

                    # 找URL
                    url = None
                    if title_el.name == 'a' and 'href' in title_el.attrs:
                        url = urljoin(journal_info['base_url'], title_el['href'])
                    else:
                        link_el = element.select_one('a[href*="/articles/"], a[href*="/article/"]')
                        if link_el:
                            url = urljoin(journal_info['base_url'], link_el['href'])

                    if not url:
                        continue

                    articles.append({
                        'title': title,
                        'url': url,
                        'journal': journal_info['name'],
                        'source': 'nature'
                    })

            except Exception as e:
                logger.error(f"提取文章元素时出错: {e}")

        return articles

    def _get_latest_articles_direct(self, journal_id):
        """直接从期刊页面获取最新文章 - 备选方案"""
        journal_info = self.journals.get(journal_id)
        if not journal_info:
            logger.error(f"未知的期刊ID: {journal_id}")
            return []

        try:
            # 直接访问期刊主页而不是搜索页面
            journal_url = journal_info['base_url']
            logger.info(f"尝试直接从期刊主页获取文章: {journal_url}")

            html_content = self.browser.get_page(
                journal_url,
                use_selenium=True,
                wait_time=20
            )

            if not html_content:
                logger.error(f"获取期刊主页失败: {journal_url}")
                return []

            # 保存期刊主页HTML以供分析
            debug_dir = "debug_html"
            os.makedirs(debug_dir, exist_ok=True)
            direct_file = os.path.join(debug_dir,
                                       f"{journal_info['name'].replace(' ', '_')}_direct_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html")
            with open(direct_file, "w", encoding="utf-8") as f:
                f.write(html_content)
            logger.info(f"已保存期刊主页HTML: {direct_file}")

            soup = BeautifulSoup(html_content, 'html.parser')

            # 查找最新文章部分
            latest_sections = [
                'section.c-latest-content',  # 常见的最新内容区域
                'div.c-latest-content',
                'section.latest-articles',
                'div.latest-articles',
                'section[data-track-action="view latest articles"]',
                'div.c-card-collection',  # 文章集合
                'ul.app-article-list'  # 文章列表
            ]

            articles = []

            # 尝试各种可能的最新文章区域
            for section_selector in latest_sections:
                sections = soup.select(section_selector)

                if sections:
                    logger.info(f"找到最新文章区域: {section_selector}, 数量: {len(sections)}")

                    for section in sections:
                        # 查找区域内的所有文章链接
                        links = section.select('a[href*="/articles/"], a[href*="/article/"]')

                        for link in links:
                            href = link.get('href', '')
                            # 跳过补充材料链接
                            if 'supplementary' in href.lower():
                                continue

                            title = link.text.strip()
                            if not title:
                                # 尝试查找链接元素内或附近的标题
                                title_el = link.select_one('h1, h2, h3, h4, h5, .title')
                                if title_el:
                                    title = title_el.text.strip()
                                else:
                                    # 往上查找父元素中的标题
                                    parent = link.parent
                                    while parent and parent.name != 'body':
                                        title_el = parent.select_one('h1, h2, h3, h4, h5, .title')
                                        if title_el:
                                            title = title_el.text.strip()
                                            break
                                        parent = parent.parent

                            if title:
                                article_url = urljoin(journal_url, href)
                                article = {
                                    'title': title,
                                    'url': article_url,
                                    'journal': journal_info['name'],
                                    'source': 'nature',
                                    'found_via': 'direct'
                                }
                                articles.append(article)

            # 如果没有找到特定区域，尝试找所有可能的文章链接
            if not articles:
                logger.info("尝试查找所有可能的文章链接")
                all_links = soup.select('a[href*="/articles/"], a[href*="/article/"]')

                for link in all_links:
                    href = link.get('href', '')
                    if 'supplementary' in href.lower():
                        continue

                    # 检查是否是真正的文章链接
                    if re.search(r'/articles?/[^/]+/?$', href):
                        title = link.text.strip()
                        # 如上面的逻辑尝试提取标题

                        if title:
                            article_url = urljoin(journal_url, href)
                            article = {
                                'title': title,
                                'url': article_url,
                                'journal': journal_info['name'],
                                'source': 'nature',
                                'found_via': 'direct_all'
                            }
                            articles.append(article)

            logger.info(f"直接从{journal_info['name']}页面找到 {len(articles)} 篇文章")
            return articles

        except Exception as e:
            logger.error(f"直接从期刊获取文章失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
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