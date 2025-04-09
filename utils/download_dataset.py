#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import logging
import argparse
from datetime import datetime

# 确保可以导入项目包
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database.operations import initialize_db, get_datasets_for_download, update_dataset_download_status, \
    get_dataset_statistics
from utils.dataset_downloader import DatasetDownloader
from config import load_config

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("dataset_download.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description='NeuroCrawler 数据集下载工具')
    parser.add_argument('--config', type=str, default='config.yaml', help='配置文件路径')
    parser.add_argument('--limit', type=int, default=10, help='下载数据集的数量限制')
    parser.add_argument('--include-downloaded', action='store_true', help='包括已下载的数据集')
    parser.add_argument('--platform', type=str, help='指定下载平台（如figshare, zenodo等）')
    parser.add_argument('--stats', action='store_true', help='只显示统计信息，不执行下载')
    return parser.parse_args()


def main():
    args = parse_args()

    # 加载配置
    config = load_config(args.config)

    # 初始化数据库
    initialize_db(config['database'])

    # 如果只需显示统计
    if args.stats:
        stats = get_dataset_statistics()
        print("\n===== 数据集统计 =====")
        print(f"总数据集数量: {stats.get('total', 0)}")
        print(f"已下载数据集: {stats.get('downloaded', 0)}")

        if 'by_platform' in stats:
            print("\n按平台分布:")
            for platform, count in stats['by_platform'].items():
                print(f"  - {platform or '未知'}: {count}")

        print(f"\n总下载大小: {stats.get('total_download_size', 0) / (1024 * 1024):.2f} MB")
        return

    # 获取待下载的数据集
    filter_args = {
        'limit': args.limit,
        'include_downloaded': args.include_downloaded
    }

    datasets = get_datasets_for_download(**filter_args)

    if args.platform:
        datasets = [d for d in datasets if d.get('platform') == args.platform]

    if not datasets:
        logger.info("没有符合条件的数据集需要下载")
        return

    logger.info(f"找到 {len(datasets)} 个待下载的数据集")

    # 初始化下载器
    downloader = DatasetDownloader(config.get('dataset_download', {}))

    # 执行下载
    for dataset in datasets:
        logger.info(f"正在下载数据集: {dataset.get('name')} ({dataset.get('url')})")

        download_result = downloader.download_dataset(dataset)

        # 更新下载状态
        dataset_id = dataset.get('id')
        if dataset_id:
            update_dataset_download_status(dataset_id, download_result)

        if download_result.get('success'):
            logger.info(f"成功下载数据集: {dataset.get('name')}")
        elif download_result.get('status') == 'skipped':
            logger.info(f"跳过已下载的数据集: {dataset.get('name')}")
        else:
            logger.error(f"下载数据集失败: {dataset.get('name')} - {download_result.get('error')}")

    # 显示下载结果
    print("\n===== 下载结果 =====")
    success_count = sum(
        1 for d in datasets if downloader.download_history.get(d.get('id') or d.get('doi') or d.get('url')))
    print(f"总数据集: {len(datasets)}")
    print(f"成功下载: {success_count}")
    print(f"未下载/失败: {len(datasets) - success_count}")


if __name__ == "__main__":
    start_time = datetime.now()
    print(f"开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        main()
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        logger.exception("程序执行过程中发生错误:")
        print(f"\n程序执行出错: {e}")

    end_time = datetime.now()
    print(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"总耗时: {(end_time - start_time).total_seconds():.2f} 秒")