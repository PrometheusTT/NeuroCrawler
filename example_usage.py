#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import argparse
from datetime import datetime
from collectors.nature import NatureCollector
from collectors.science import ScienceCollector
from collectors.cell import CellCollector
from database.operations import initialize_db, save_papers, save_datasets
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
    parser = argparse.ArgumentParser(description='神经科学数据爬取系统示例')
    parser.add_argument('--config', type=str, default='config.yaml', help='配置文件路径')
    parser.add_argument('--source', type=str, help='只爬取指定来源 (nature, science, cell)')
    parser.add_argument('--days', type=int, default=10, help='爬取过去的天数（覆盖第一次运行的默认值）')
    parser.add_argument('--debug', action='store_true', help='启用调试模式，保存HTML页面')
    return parser.parse_args()


def main():
    args = parse_args()
    config = load_config(args.config)

    # 初始化数据库
    initialize_db(config['database'])

    # 确定要运行哪些爬虫
    collectors_to_run = []

    if args.source:
        if args.source.lower() == 'nature':
            collectors_to_run.append(('nature', NatureCollector(config['sources']['nature'])))
        elif args.source.lower() == 'science':
            collectors_to_run.append(('science', ScienceCollector(config['sources']['science'])))
        elif args.source.lower() == 'cell':
            collectors_to_run.append(('cell', CellCollector(config['sources']['cell'])))
        elif args.source.lower() == 'arxiv':
            collectors_to_run.append(('arxiv', CellCollector(config['sources']['arxiv'])))
        else:
            logger.error(f"未知的来源: {args.source}")
            return
    else:
        # 运行所有爬虫
        collectors_to_run = [
            ('nature', NatureCollector(config['sources']['nature'])),
            ('science', ScienceCollector(config['sources']['science'])),
            ('cell', CellCollector(config['sources']['cell']))
        ]

    all_papers = []
    all_datasets = []

    # 运行爬虫
    for name, collector in collectors_to_run:
        logger.info(f"开始爬取 {name} 的神经科学数据")

        try:
            # 收集论文
            papers = collector.collect_papers()
            logger.info(f"从 {name} 收集到 {len(papers)} 篇论文")

            # 提取数据集
            for paper in papers:
                datasets = collector.extract_datasets(paper)
                paper['datasets'] = datasets
                all_datasets.extend(datasets)

            all_papers.extend(papers)

        except Exception as e:
            logger.error(f"爬取 {name} 时出错: {e}")

    # 保存结果到数据库
    if all_papers:
        save_papers(all_papers)
        logger.info(f"保存了 {len(all_papers)} 篇论文")

    if all_datasets:
        save_datasets(all_datasets)
        logger.info(f"保存了 {len(all_datasets)} 个数据集")

    # 打印汇总信息
    print("\n爬取结果汇总:")
    print(f"总共爬取论文: {len(all_papers)} 篇")
    print(f"总共提取数据集: {len(all_datasets)} 个")

    # 按数据类型分类统计
    data_type_counts = {}
    for dataset in all_datasets:
        for data_type in dataset.get('data_types', []):
            if data_type in data_type_counts:
                data_type_counts[data_type] += 1
            else:
                data_type_counts[data_type] = 1

    print("\n数据类型分布:")
    for data_type, count in data_type_counts.items():
        print(f"  - {data_type}: {count} 个数据集")

    # 按数据仓库分类统计
    repo_counts = {}
    for dataset in all_datasets:
        repo = dataset.get('repository', 'unknown')
        if repo in repo_counts:
            repo_counts[repo] += 1
        else:
            repo_counts[repo] = 1

    print("\n数据仓库分布:")
    for repo, count in repo_counts.items():
        print(f"  - {repo}: {count} 个数据集")


if __name__ == "__main__":
    main()