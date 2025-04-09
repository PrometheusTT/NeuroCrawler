#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import argparse
from datetime import datetime, timedelta
from config import load_config
from database.operations import initialize_db, get_datasets_by_criteria
from utils.dataset_downloader import DatasetDownloadManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("download.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description='NeuroCrawler: 数据集下载示例')
    parser.add_argument('--config', type=str, default='config.yaml', help='配置文件路径')
    parser.add_argument('--days', type=int, default=30, help='下载过去N天的数据集')
    parser.add_argument('--max', type=int, default=5, help='最大下载数量')
    parser.add_argument('--type', type=str, nargs='+',
                        choices=['neuron_imaging', 'reconstruction', 'spatial_transcriptomics', 'mri',
                                 'electrophysiology'],
                        help='下载指定类型的数据集')
    parser.add_argument('--source', type=str, nargs='+',
                        choices=['nature', 'science', 'cell', 'arxiv', 'biorxiv'],
                        help='从指定来源下载数据集')
    parser.add_argument('--download-dir', type=str, help='下载目录路径')
    return parser.parse_args()


def main():
    args = parse_args()
    config = load_config(args.config)

    # 初始化数据库
    initialize_db(config['database'])

    # 更新下载目录配置
    if args.download_dir:
        config['downloader'] = config.get('downloader', {})
        config['downloader']['download_dir'] = args.download_dir

    # 初始化下载管理器
    download_manager = DatasetDownloadManager(config)

    # 从数据库获取符合条件的数据集
    logger.info(f"正在从数据库查询符合条件的数据集...")

    # 构建查询条件
    days = args.days
    data_types = args.type
    sources = args.source
    max_datasets = args.max

    # 获取数据集
    datasets = get_datasets_by_criteria(
        days=days,
        data_types=data_types,
        sources=sources,
        limit=max_datasets
    )

    if not datasets:
        logger.info("未找到符合条件的数据集")
        return

    logger.info(f"找到 {len(datasets)} 个符合条件的数据集")

    # 显示将要下载的数据集
    print("\n将要下载的数据集:")
    for i, dataset in enumerate(datasets, 1):
        types = ", ".join(dataset.get('data_types', ["未知"]))
        print(f"{i}. {dataset['name']} ({types}) - 来源: {dataset['source']}")
        print(f"   URL: {dataset['url'] or '无'}")
        if dataset.get('paper_title'):
            print(f"   论文: {dataset.get('paper_title')}")
        print()

    # 确认下载
    confirm = input(f"确认下载这些数据集? (y/n): ")
    if confirm.lower() != 'y':
        print("下载已取消")
        return

    # 执行下载
    logger.info("开始下载数据集...")
    results = download_manager.downloader.download_datasets(datasets)

    # 显示下载结果
    print(f"\n下载完成! 成功: {results['success']}, 失败: {results['failed']}, 跳过: {results['skipped']}")

    if results['details']:
        print("\n详细结果:")
        for detail in results['details']:
            status = "成功" if detail.get('success') else "失败" if 'error' in detail else "跳过"
            print(f"- {detail.get('dataset', 'Unknown')}: {status}")
            if detail.get('path'):
                print(f"  保存路径: {detail.get('path')}")
            if detail.get('error'):
                print(f"  错误: {detail.get('error')}")


if __name__ == "__main__":
    main()