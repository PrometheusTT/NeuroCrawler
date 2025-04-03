#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import argparse
from datetime import datetime
from collectors.arxiv import ArxivCollector
from collectors.biorxiv import BiorxivCollector
from collectors.nature import NatureCollector
from collectors.science import ScienceCollector
from collectors.cell import CellCollector
from collectors.github import GitHubCollector
from database.operations import initialize_db, save_papers, save_datasets, save_repositories
from scheduler import Scheduler
from notifier import Notifier
from config import load_config

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("neurocrawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description='NeuroCrawler: 神经科学数据爬取系统')
    parser.add_argument('--config', type=str, default='config.yaml', help='配置文件路径')
    parser.add_argument('--once', action='store_true', help='只执行一次，不启动调度器')
    parser.add_argument('--source', type=str, help='只爬取指定来源 (arxiv, biorxiv, nature, science, cell, github)')
    return parser.parse_args()


def main():
    args = parse_args()
    config = load_config(args.config)

    # 初始化数据库
    initialize_db(config['database'])

    # 初始化爬虫
    collectors = {
        'arxiv': ArxivCollector(config['sources']['arxiv']),
        'biorxiv': BiorxivCollector(config['sources']['biorxiv']),
        'nature': NatureCollector(config['sources']['nature']),
        'science': ScienceCollector(config['sources']['science']),
        'cell': CellCollector(config['sources']['cell']),
        'github': GitHubCollector(config['sources']['github'])
    }

    # 初始化通知系统
    notifier = Notifier(config['notification'])

    # 执行爬取任务
    def crawl_task():
        logger.info(f"开始爬取任务: {datetime.now()}")

        # 如果指定了特定来源，只爬取该来源
        sources_to_crawl = [args.source] if args.source else collectors.keys()

        all_papers = []
        all_datasets = []
        all_repos = []

        # 爬取论文和数据集
        for source in sources_to_crawl:
            if source != 'github':  # GitHub爬虫单独处理
                try:
                    logger.info(f"从 {source} 爬取数据")
                    collector = collectors[source]
                    papers = collector.collect_papers()
                    all_papers.extend(papers)

                    # 从论文中提取数据集信息
                    for paper in papers:
                        datasets = collector.extract_datasets(paper)
                        all_datasets.extend(datasets)

                    logger.info(f"从 {source} 爬取了 {len(papers)} 篇论文和 {len(all_datasets)} 个数据集")
                except Exception as e:
                    logger.error(f"爬取 {source} 时出错: {e}")

        # 保存论文和数据集
        if all_papers:
            save_papers(all_papers)
            notifier.notify_new_papers(all_papers)

        if all_datasets:
            save_datasets(all_datasets)
            notifier.notify_new_datasets(all_datasets)

        # 爬取相关GitHub仓库
        if 'github' in sources_to_crawl or not args.source:
            try:
                # 根据论文中提取的GitHub链接和关键词爬取仓库
                github_collector = collectors['github']
                repos = github_collector.collect_repositories(all_papers)
                all_repos.extend(repos)

                # 保存仓库信息
                if all_repos:
                    save_repositories(all_repos)
                    notifier.notify_new_repositories(all_repos)

                logger.info(f"爬取了 {len(all_repos)} 个GitHub仓库")
            except Exception as e:
                logger.error(f"爬取GitHub仓库时出错: {e}")

        logger.info(f"爬取任务完成: {datetime.now()}")

    # 执行一次爬取
    crawl_task()

    # 如果不是只执行一次，启动调度器
    if not args.once:
        scheduler = Scheduler(config['scheduler'])
        scheduler.add_job(crawl_task)
        scheduler.start()


if __name__ == "__main__":
    main()