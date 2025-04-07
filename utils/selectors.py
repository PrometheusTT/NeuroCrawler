#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
集中管理各个期刊网站的HTML选择器
便于当网站结构更新时统一修改
"""

# Nature及其子刊选择器
NATURE_SELECTORS = {
    # 搜索结果页
    'article_items': [
        'li.app-article-list-row',         # 旧版选择器
        'li.c-list-group__item',           # 可能的新选择器
        'article.u-full-width',            # 可能的新选择器
        'div.c-card.c-card--flush',        # 可能的新选择器
        'div.u-flex-justify-between',      # 可能的新选择器
        'li.has-journal-link',             # 最新版可能的选择器
        'article[data-track-action="view article"]'  # 最新版可能的选择器
    ],
    'article_title': [
        'h3.c-card__title a',              # 旧版选择器
        'a.c-card__link',                  # 可能的新选择器
        'h2 a',                            # 通用选择器
        'h3 a',                            # 通用选择器
        'a[data-track-action="view article"]', # 可能的新选择器
        'a.u-link-inherit'                 # 最新版可能的选择器
    ],
    'article_date': 'time',
    'article_authors': 'ul.c-author-list li',

    # 文章详情页
    'abstract': [
        'div#abstract',
        'div.c-article-section[data-title="Abstract"] p',
        'section#abstract p',
        'div[id*="abstract"] p',  # 更宽松的匹配
        'meta[name="description"]'  # 备选：元数据中的描述
    ],
    'doi': [
        'meta[name="DOI"]',
        'meta[name="doi"]',
        'meta[property="og:doi"]',
        'a.c-bibliographic-information__doi-link'  # 另一个可能的位置
    ],
    'pdf_link': [
        'a.c-pdf-download__link',
        'a[data-track-action="download pdf"]',
        'a[href$=".pdf"]'  # 备选：任何指向PDF的链接
    ],
    'supplementary': [
        'a[data-track-action="supplementary information"]',
        'a:contains("Supplementary")'  # 备选：包含Supplementary文本的链接
    ],

    # 数据可用性部分
    'data_availability': [
        'div.c-article-section[data-title="Data availability"]',
        'div.c-article-section[data-title="Availability"]',
        'h2:contains("Data availability") + div',
        'h2:contains("DATA AVAILABILITY") + div',
        'div.c-article-section:contains("Data availability")',  # 更宽松的匹配
        'div.c-article-section:contains("Code availability")'  # 也匹配代码可用性部分
    ]
}

# Science系列期刊选择器
SCIENCE_SELECTORS = {
    # 这里添加Science期刊的选择器
}

# Cell系列期刊选择器
CELL_SELECTORS = {
    # 这里添加Cell期刊的选择器
}