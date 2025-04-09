#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
import argparse
from datetime import datetime, timedelta
from collections import defaultdict
# from collectors.arxiv import ArxivCollector
# from collectors.biorxiv import BiorxivCollector
from collectors.nature import NatureCollector
# from collectors.science import ScienceCollector
# from collectors.cell import CellCollector
# from collectors.github import GitHubCollector
from database.operations import initialize_db, save_papers, save_datasets, save_repositories
from scheduler import Scheduler
from notifier import Notifier
from config import load_config
from utils.dataset_downloader import DatasetDownloadManager

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
    parser = argparse.ArgumentParser(description='NeuroCrawler: 神经科学数据爬取与下载系统')
    parser.add_argument('--config', type=str, default='config.yaml', help='配置文件路径')
    parser.add_argument('--once', action='store_true', help='只执行一次，不启动调度器')
    parser.add_argument('--source', type=str, help='只爬取指定来源 (arxiv, biorxiv, nature, science, cell, github)')

    # 爬取日期范围
    parser.add_argument('--start-date', type=str, help='爬取起始日期 (YYYY-MM-DD格式)')
    parser.add_argument('--end-date', type=str, help='爬取结束日期 (YYYY-MM-DD格式，默认为今天)')
    parser.add_argument('--days', type=int, help='爬取过去N天的数据（与日期范围互斥）')

    # 数据集下载控制
    parser.add_argument('--download', action='store_true', help='是否下载数据集')
    parser.add_argument('--download-max', type=int, default=10, help='最大下载数据集数量')
    parser.add_argument('--download-dir', type=str, help='数据集下载目录')
    parser.add_argument('--data-type', type=str, nargs='+',
                        help='下载指定类型的数据集 (neuron_imaging, reconstruction, spatial_transcriptomics, mri, electrophysiology)')
    parser.add_argument('--db-download', action='store_true',
                        help='从数据库下载已爬取的数据集（而不是从本次爬取结果）')
    parser.add_argument('--download-only', action='store_true',
                        help='仅执行下载操作，不进行爬取')

    # 直接下载URL
    parser.add_argument('--download-url', type=str, help='直接从指定URL下载数据集')
    parser.add_argument('--dataset-name', type=str, help='数据集名称（与--download-url一起使用）')
    parser.add_argument('--dataset-platform', type=str, help='数据集平台/仓库（与--download-url一起使用）')

    # 强制下载选项
    parser.add_argument('--force', action='store_true', help='强制重新下载，即使数据集已存在')

    # 网页处理选项
    parser.add_argument('--smart-download', action='store_true',
                        help='启用智能下载，自动提取和下载网页中的真实数据文件（默认启用）')
    parser.add_argument('--no-smart-download', action='store_true',
                        help='禁用智能下载，仅保存原始HTML而不处理')

    # 用户信息（用于日志）
    parser.add_argument('--user', type=str, default=os.environ.get('USER', 'unknown'),
                        help='用户名（用于日志记录）')

    return parser.parse_args()


def get_date_range(args):
    """解析命令行参数中的日期范围"""
    end_date = datetime.now()

    # 如果指定了结束日期，解析它
    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
        except ValueError:
            logger.error(f"结束日期格式错误: {args.end_date}，使用当前日期")

    # 确定开始日期
    if args.days:
        # 如果指定了天数，从结束日期向前推
        start_date = end_date - timedelta(days=args.days)
    elif args.start_date:
        # 如果指定了开始日期，解析它
        try:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        except ValueError:
            logger.error(f"开始日期格式错误: {args.start_date}，使用30天前")
            start_date = end_date - timedelta(days=30)
    else:
        # 默认使用30天前
        start_date = end_date - timedelta(days=30)

    return start_date, end_date


def main():
    args = parse_args()
    config = load_config(args.config)

    # 初始化数据库
    initialize_db(config['database'])

    # 确保下载配置存在
    if 'downloader' not in config:
        config['downloader'] = {}

    # 设置智能下载选项（默认启用，除非显式禁用）
    if not args.no_smart_download:
        config['downloader']['smart_webpage_handling'] = True
        config['downloader']['extract_download_links'] = True
        # 如果有Selenium，尝试使用它进行复杂网页交互
        try:
            import selenium
            config['downloader']['use_selenium'] = True
            logger.info("已启用Selenium支持进行智能下载")
        except ImportError:
            config['downloader']['use_selenium'] = False
            logger.info("未检测到Selenium，复杂网页交互功能将不可用")
    else:
        config['downloader']['smart_webpage_handling'] = False
        config['downloader']['extract_download_links'] = False
        config['downloader']['use_selenium'] = False
        logger.info("已禁用智能下载功能")

    # 处理下载目录参数
    if args.download_dir:
        config['downloader']['download_dir'] = args.download_dir

    # 获取日期范围
    start_date, end_date = get_date_range(args)
    logger.info(f"日期范围: {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")

    # 初始化下载管理器（如果需要下载）
    if args.download or args.download_only or args.db_download or args.download_url:
        download_manager = DatasetDownloadManager(config)

    # 初始化通知系统
    notification_config = config.get('notification', {})
    notifier = Notifier(notification_config)

    # 打印当前设置
    logger.info(f"当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"用户: {args.user}")
    logger.info(f"智能下载功能: {'已启用' if config['downloader'].get('smart_webpage_handling') else '已禁用'}")

    # 检查是否是直接URL下载模式
    if args.download_url:
        logger.info(f"直接下载模式: {args.download_url}")

        # 设置默认值
        dataset_name = args.dataset_name or f"dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        dataset_platform = args.dataset_platform or "自动检测"

        # 记录开始下载
        print(f"\n正在尝试从 {args.download_url} 下载数据集...")
        logger.info(f"用户 {args.user} 开始下载 {args.download_url}")

        # 执行下载 - 启用智能处理，使data_utils中的特殊处理方法被调用
        success, message = download_manager.download_single_dataset(
            dataset_url=args.download_url,
            name=dataset_name,
            repository=dataset_platform,
            force=args.force
        )

        # 更详细的下载结果信息
        if success:
            result_message = f"数据集下载成功: {message}"
            logger.info(result_message)
            print(f"\n✅ {result_message}")

            # 显示下载目录并列出文件
            download_dir = os.path.join(config['downloader'].get('download_dir', 'datasets'), dataset_name)
            print(f"保存位置: {download_dir}")

            # 列出下载的文件
            try:
                if os.path.exists(download_dir):
                    files = [f for f in os.listdir(download_dir) if os.path.isfile(os.path.join(download_dir, f))]
                    if files:
                        print(f"\n下载的文件:")
                        for i, file in enumerate(files, 1):
                            file_path = os.path.join(download_dir, file)
                            file_size = os.path.getsize(file_path)
                            size_str = f"{file_size / 1024 / 1024:.2f} MB" if file_size > 1024 * 1024 else f"{file_size / 1024:.2f} KB"
                            print(f"  {i}. {file} ({size_str})")
            except Exception as e:
                logger.error(f"列出下载文件时出错: {e}")

            download_results = {"success": 1, "failed": 0, "skipped": 0, "total": 1}
        else:
            result_message = f"数据集下载失败: {message}"
            logger.error(result_message)
            print(f"\n❌ {result_message}")
            print("请尝试使用 --force 参数强制重新下载，或检查URL是否正确")
            download_results = {"success": 0, "failed": 1, "skipped": 0, "total": 1}

        # 通知下载结果
        notifier.notify_download_results(download_results)
        return

    # 如果只是从数据库下载数据集，不执行爬取任务
    if args.download_only or (args.db_download and args.download):
        logger.info("仅执行数据集下载，跳过爬取过程")

        data_types = args.data_type if args.data_type else None
        sources = [args.source] if args.source else None

        # 从数据库获取并下载数据集
        logger.info("开始从数据库下载数据集")
        download_results = download_manager.download_datasets_from_database(
            start_date=start_date,
            end_date=end_date,
            max_datasets=args.download_max,
            data_types=data_types,
            sources=sources
        )

        logger.info(f"数据集下载完成: 成功 {download_results['success']}, "
                    f"失败 {download_results['failed']}, 跳过 {download_results['skipped']}")

        # 打印下载结果摘要
        print(f"\n数据集下载完成:")
        print(f"成功: {download_results['success']} 个")
        print(f"失败: {download_results['failed']} 个")
        print(f"跳过: {download_results['skipped']} 个")
        print(f"总计: {download_results['total']} 个")

        # 通知下载结果
        notifier.notify_download_results(download_results)
        return

    # 如果不是只下载模式，初始化爬虫
    collectors = {
        # 'arxiv': ArxivCollector(config['sources']['arxiv']),
        # 'biorxiv': BiorxivCollector(config['sources']['biorxiv']),
        'nature': NatureCollector(config['sources']['nature']),
        # 'science': ScienceCollector(config['sources']['science']),
        # 'cell': CellCollector(config['sources']['cell']),
        # 'github': GitHubCollector(config['sources']['github'])
    }

    # 执行爬取任务
    def crawl_task():
        logger.info(f"开始爬取任务: {datetime.now()}")

        # 如果指定了特定来源，只爬取该来源
        sources_to_crawl = [args.source] if args.source else collectors.keys()

        all_papers = []
        all_datasets = []
        all_repos = []

        # 每个来源提取的数据集数量统计
        dataset_stats = defaultdict(int)

        # 爬取论文和数据集
        for source in sources_to_crawl:
            if source != 'github':  # GitHub爬虫单独处理
                try:
                    logger.info(f"从 {source} 爬取数据")
                    collector = collectors[source]

                    # 传入日期范围参数
                    papers = collector.collect_papers(start_date=start_date, end_date=end_date)
                    all_papers.extend(papers)

                    # 从论文中提取数据集信息
                    for paper in papers:
                        datasets = collector.extract_datasets(paper)
                        if datasets:
                            # 记录来源统计
                            dataset_stats[source] += len(datasets)
                            all_datasets.extend(datasets)

                    logger.info(f"从 {source} 爬取了 {len(papers)} 篇论文和 {dataset_stats[source]} 个数据集")
                except Exception as e:
                    logger.error(f"爬取 {source} 时出错: {e}")

        # 保存论文和数据集
        if all_papers:
            saved_paper_count = save_papers(all_papers)
            logger.info(f"保存了 {saved_paper_count} 篇论文到数据库")
            notifier.notify_new_papers(all_papers)

        if all_datasets:
            saved_dataset_count = save_datasets(all_datasets)
            logger.info(f"保存了 {saved_dataset_count} 个数据集到数据库")
            notifier.notify_new_datasets(all_datasets)

        # 爬取相关GitHub仓库
        if 'github' in sources_to_crawl or not args.source:
            try:
                # 根据论文中提取的GitHub链接和关键词爬取仓库
                github_collector = collectors['github']
                repos = github_collector.collect_repositories(all_papers, start_date=start_date, end_date=end_date)
                all_repos.extend(repos)

                # 保存仓库信息
                if all_repos:
                    saved_repo_count = save_repositories(all_repos)
                    logger.info(f"保存了 {saved_repo_count} 个GitHub仓库到数据库")
                    notifier.notify_new_repositories(all_repos)

                logger.info(f"爬取了 {len(all_repos)} 个GitHub仓库")
            except Exception as e:
                logger.error(f"爬取GitHub仓库时出错: {e}")

        logger.info(f"爬取任务完成: {datetime.now()}")
        logger.info(f"数据集来源统计: {dict(dataset_stats)}")

        # 处理数据集下载
        if args.download and download_manager:
            logger.info("开始下载数据集")
            data_types = args.data_type if args.data_type else None

            # 打印数据集类型信息
            if data_types:
                logger.info(f"过滤数据类型: {', '.join(data_types)}")

            if args.db_download:
                # 从数据库下载之前爬取的数据集
                logger.info("从数据库获取并下载数据集")
                download_results = download_manager.download_datasets_from_database(
                    start_date=start_date,
                    end_date=end_date,
                    max_datasets=args.download_max,
                    data_types=data_types,
                    sources=[args.source] if args.source else None
                )
            else:
                # 下载本次爬取到的数据集
                if all_datasets:
                    print(f"\n开始下载爬取到的数据集...")
                    logger.info(f"准备下载 {min(len(all_datasets), args.download_max)} 个数据集")

                    # 使用增强的下载功能，自动处理网页类数据集
                    download_results = download_manager.download_datasets_from_crawler_results(
                        all_papers,
                        max_datasets=args.download_max,
                        data_types=data_types
                    )
                else:
                    logger.info("没有找到符合条件的数据集")
                    download_results = {"success": 0, "failed": 0, "skipped": 0, "total": 0}

            logger.info(f"数据集下载完成: 成功 {download_results['success']}, "
                        f"失败 {download_results['failed']}, 跳过 {download_results['skipped']}")

            # 打印下载结果摘要
            print(f"\n数据集下载完成:")
            print(f"成功: {download_results['success']} 个")
            print(f"失败: {download_results['failed']} 个")
            print(f"跳过: {download_results['skipped']} 个")
            print(f"总计: {download_results['total']} 个")

            if download_results['success'] > 0:
                print(
                    f"\n已成功下载 {download_results['success']} 个数据集，保存在 {config['downloader'].get('download_dir', 'datasets')} 目录")

            # 通知下载结果
            notifier.notify_download_results(download_results)

    # 执行一次爬取
    crawl_task()

    # 如果不是只执行一次，启动调度器
    if not args.once:
        scheduler = Scheduler(config.get('scheduler', {}))
        scheduler.add_job(crawl_task)
        scheduler.start()


if __name__ == "__main__":
    main()