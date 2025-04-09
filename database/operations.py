#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import json
import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, or_, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from database.models import Base, Paper, Dataset, Repository, paper_dataset, paper_repository

logger = logging.getLogger(__name__)

# 全局Session对象
Session = None
engine = None


def initialize_db(db_config):
    """
    初始化数据库连接

    Args:
        db_config: 数据库配置字典
    """
    global Session, engine

    db_type = db_config.get('type', 'sqlite')

    if db_type == 'sqlite':
        db_path = db_config.get('path', 'neurocrawler.db')
        db_url = f'sqlite:///{db_path}'
    elif db_type == 'mysql':
        host = db_config.get('host', 'localhost')
        port = db_config.get('port', 3306)
        user = db_config.get('user', 'root')
        password = db_config.get('password', '')
        database = db_config.get('database', 'neurocrawler')
        db_url = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
    elif db_type == 'postgresql':
        host = db_config.get('host', 'localhost')
        port = db_config.get('port', 5432)
        user = db_config.get('user', 'postgres')
        password = db_config.get('password', '')
        database = db_config.get('database', 'neurocrawler')
        db_url = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    else:
        raise ValueError(f"不支持的数据库类型: {db_type}")

    # 创建引擎和会话
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    logger.info(f"数据库初始化完成: {db_url}")


def save_papers(papers):
    """
    保存论文到数据库

    Args:
        papers: 论文列表

    Returns:
        int: 保存的论文数量
    """
    if not papers:
        return 0

    session = Session()
    count = 0

    try:
        for paper_data in papers:
            # 检查论文是否已存在
            existing = None

            # 按DOI查找（如果有）
            if 'doi' in paper_data and paper_data['doi']:
                existing = session.query(Paper).filter_by(doi=paper_data['doi']).first()

            # 如果没有DOI或未找到，按URL查找
            if not existing and 'url' in paper_data:
                existing = session.query(Paper).filter_by(url=paper_data['url']).first()

            # 如果没有URL或未找到，按外部ID查找
            if not existing and 'external_id' in paper_data and paper_data['external_id']:
                existing = session.query(Paper).filter_by(external_id=paper_data['external_id']).first()

            if existing:
                # 更新已有记录
                for key, value in paper_data.items():
                    if key not in ['id', 'datasets', 'authors', 'keywords', 'topics', 'repositories'] and hasattr(
                            existing, key):
                        setattr(existing, key, value)

                logger.debug(f"更新论文: {paper_data.get('title', 'Unknown')}")
                paper = existing
            else:
                # 创建新论文
                paper = Paper()

                # 填充基本字段
                for key, value in paper_data.items():
                    if key not in ['id', 'datasets', 'authors', 'keywords', 'topics', 'repositories'] and hasattr(paper,
                                                                                                                  key):
                        setattr(paper, key, value)

                session.add(paper)
                logger.debug(f"添加新论文: {paper_data.get('title', 'Unknown')}")

            session.flush()  # 确保paper有ID

            # 处理数据集关联
            if 'datasets' in paper_data and paper_data['datasets']:
                for dataset_data in paper_data['datasets']:
                    save_dataset(session, dataset_data, paper)

            count += 1

        session.commit()
        logger.info(f"成功保存 {count} 篇论文")
        return count

    except Exception as e:
        session.rollback()
        logger.error(f"保存论文时出错: {e}")
        return 0

    finally:
        session.close()


def save_dataset(session, dataset_data, paper=None):
    """
    保存或更新数据集

    Args:
        session: 数据库会话
        dataset_data: 数据集数据
        paper: 关联的论文对象

    Returns:
        Dataset: 保存的数据集对象
    """
    # 尝试按URL找到数据集
    existing = None
    if 'url' in dataset_data and dataset_data['url']:
        existing = session.query(Dataset).filter_by(url=dataset_data['url']).first()

    # 如果没找到并且有DOI，按DOI查找
    if not existing and 'doi' in dataset_data and dataset_data['doi']:
        existing = session.query(Dataset).filter_by(doi=dataset_data['doi']).first()

    if existing:
        # 更新现有数据集
        for key, value in dataset_data.items():
            if key != 'id' and hasattr(existing, key):
                setattr(existing, key, value)

        dataset = existing
        logger.debug(f"更新数据集: {dataset_data.get('name', 'Unknown')}")
    else:
        # 创建新数据集
        dataset = Dataset()

        # 填充字段
        for key, value in dataset_data.items():
            if key != 'id' and hasattr(dataset, key):
                setattr(dataset, key, value)

        # 设置默认值
        if not hasattr(dataset, 'crawled_date') or not dataset.crawled_date:
            dataset.crawled_date = datetime.now()

        session.add(dataset)
        logger.debug(f"添加新数据集: {dataset_data.get('name', 'Unknown')}")

    # 如果提供了论文，建立关联
    if paper and paper not in dataset.papers:
        dataset.papers.append(paper)

    return dataset


def save_datasets(datasets):
    """
    批量保存数据集

    Args:
        datasets: 数据集列表

    Returns:
        int: 保存的数据集数量
    """
    if not datasets:
        return 0

    session = Session()
    count = 0

    try:
        for dataset_data in datasets:
            # 保存数据集，不关联论文
            save_dataset(session, dataset_data)
            count += 1

        session.commit()
        logger.info(f"成功保存 {count} 个数据集")
        return count

    except Exception as e:
        session.rollback()
        logger.error(f"保存数据集时出错: {e}")
        return 0

    finally:
        session.close()


def save_repositories(repositories):
    """
    保存GitHub仓库

    Args:
        repositories: 仓库列表

    Returns:
        int: 保存的仓库数量
    """
    if not repositories:
        return 0

    session = Session()
    count = 0

    try:
        for repo_data in repositories:
            # 构建full_name（如果没有）
            if 'full_name' not in repo_data and 'owner' in repo_data and 'name' in repo_data:
                repo_data['full_name'] = f"{repo_data['owner']}/{repo_data['name']}"

            # 查找已有仓库
            existing = None
            if 'full_name' in repo_data:
                existing = session.query(Repository).filter_by(full_name=repo_data['full_name']).first()

            if not existing and 'url' in repo_data:
                existing = session.query(Repository).filter_by(url=repo_data['url']).first()

            if existing:
                # 更新现有仓库
                for key, value in repo_data.items():
                    if key not in ['id', 'papers'] and hasattr(existing, key):
                        setattr(existing, key, value)

                repo = existing
                logger.debug(f"更新仓库: {repo_data.get('full_name', 'Unknown')}")
            else:
                # 创建新仓库
                repo = Repository()

                # 填充字段
                for key, value in repo_data.items():
                    if key not in ['id', 'papers'] and hasattr(repo, key):
                        setattr(repo, key, value)

                session.add(repo)
                logger.debug(f"添加新仓库: {repo_data.get('full_name', 'Unknown')}")

            # 关联论文（如果有）
            if 'referenced_in' in repo_data and isinstance(repo_data['referenced_in'], dict):
                paper_info = repo_data['referenced_in']

                # 尝试查找引用的论文
                paper = None

                if 'paper_id' in paper_info and paper_info['paper_id']:
                    paper = session.query(Paper).get(paper_info['paper_id'])

                if not paper and 'paper_doi' in paper_info and paper_info['paper_doi']:
                    paper = session.query(Paper).filter_by(doi=paper_info['paper_doi']).first()

                if not paper and 'paper_url' in paper_info and paper_info['paper_url']:
                    paper = session.query(Paper).filter_by(url=paper_info['paper_url']).first()

                # 关联论文
                if paper and paper not in repo.papers:
                    repo.papers.append(paper)

            count += 1

        session.commit()
        logger.info(f"成功保存 {count} 个GitHub仓库")
        return count

    except Exception as e:
        session.rollback()
        logger.error(f"保存GitHub仓库时出错: {e}")
        return 0

    finally:
        session.close()


def get_datasets_by_criteria(start_date=None, end_date=None, days=None,
                             data_types=None, sources=None, limit=None):
    """
    根据条件从数据库获取数据集 - 适配多对多关系模型

    Args:
        start_date: 开始日期
        end_date: 结束日期
        days: 过去几天
        data_types: 数据类型列表
        sources: 来源列表
        limit: 最大返回数量

    Returns:
        list: 数据集列表
    """
    session = Session()

    try:
        # 基础查询 - 查询数据集和关联的论文
        query = session.query(Dataset, Paper). \
            join(paper_dataset, Dataset.id == paper_dataset.c.dataset_id). \
            join(Paper, Paper.id == paper_dataset.c.paper_id)

        # 应用日期过滤
        if days is not None:
            start_date = datetime.now() - timedelta(days=days)
            query = query.filter(Paper.published_date >= start_date)
        elif start_date is not None and end_date is not None:
            query = query.filter(Paper.published_date.between(start_date, end_date))

        # 应用数据类型过滤
        if data_types:
            type_filters = []
            for data_type in data_types:
                # 根据数据库类型选择合适的JSON查询方式
                if engine.name == 'sqlite':
                    # SQLite JSON查询
                    type_filters.append(
                        func.json_extract(Dataset.data_types, '$').like(f'%{data_type}%')
                    )
                elif engine.name == 'postgresql':
                    # PostgreSQL JSON查询
                    type_filters.append(
                        Dataset.data_types.op('?')(data_type)
                    )
                elif engine.name == 'mysql':
                    # MySQL JSON查询
                    type_filters.append(
                        func.json_contains(Dataset.data_types, f'"{data_type}"')
                    )
                else:
                    # 默认使用LIKE
                    type_filters.append(
                        Dataset.data_types.cast(String).like(f'%{data_type}%')
                    )

            if type_filters:
                query = query.filter(or_(*type_filters))

        # 应用来源过滤
        if sources:
            query = query.filter(Paper.source.in_(sources))

        # 应用结果限制
        if limit:
            query = query.limit(limit)

        # 执行查询
        results = query.all()

        # 处理结果
        datasets = []
        for dataset, paper in results:
            # 转换数据集对象为字典
            dataset_dict = {
                'id': dataset.id,
                'name': dataset.name,
                'description': dataset.description,
                'url': dataset.url,
                'doi': dataset.doi,
                'source': dataset.source,
                'platform': dataset.platform,  # 这对应于旧模型中的 repository
                'size': dataset.size,
                'format': dataset.format,
                'license': dataset.license,
                'data_types': dataset.data_types,
                'extra_metadata': dataset.extra_metadata,

                # 添加关联论文信息
                'paper_title': paper.title,
                'paper_url': paper.url,
                'paper_doi': paper.doi
            }

            # 确保所有字段都是字符串类型
            for key in ['url', 'name', 'platform', 'paper_title', 'paper_url', 'paper_doi']:
                if key in dataset_dict and dataset_dict[key] is not None:
                    dataset_dict[key] = str(dataset_dict[key])

            # 处理data_types字段
            if dataset_dict['data_types'] and isinstance(dataset_dict['data_types'], str):
                try:
                    dataset_dict['data_types'] = json.loads(dataset_dict['data_types'])
                except:
                    dataset_dict['data_types'] = []

            # 添加下载器期望的repository字段（对应于platform）
            dataset_dict['repository'] = dataset_dict.get('platform', 'unknown')

            datasets.append(dataset_dict)

        logger.info(f"查询到 {len(datasets)} 个符合条件的数据集")
        return datasets

    except Exception as e:
        logger.error(f"从数据库获取数据集时出错: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []

    finally:
        session.close()


def get_papers_by_criteria(start_date=None, end_date=None, days=None,
                           sources=None, keywords=None, limit=None):
    """
    根据条件获取论文

    Args:
        start_date: 开始日期
        end_date: 结束日期
        days: 过去几天
        sources: 来源列表
        keywords: 关键词列表
        limit: 最大返回数量

    Returns:
        list: 论文列表
    """
    session = Session()

    try:
        query = session.query(Paper)

        # 日期过滤
        if days is not None:
            start_date = datetime.now() - timedelta(days=days)
            query = query.filter(Paper.published_date >= start_date)
        elif start_date is not None and end_date is not None:
            query = query.filter(Paper.published_date.between(start_date, end_date))

        # 来源过滤
        if sources:
            query = query.filter(Paper.source.in_(sources))

        # 关键词过滤
        if keywords:
            from sqlalchemy import or_
            filters = []
            for keyword in keywords:
                filters.extend([
                    Paper.title.ilike(f'%{keyword}%'),
                    Paper.abstract.ilike(f'%{keyword}%')
                ])
            query = query.filter(or_(*filters))

        # 排序
        query = query.order_by(Paper.published_date.desc())

        # 限制返回数量
        if limit:
            query = query.limit(limit)

        # 执行查询并转换为字典
        papers = []
        for paper in query.all():
            paper_dict = {
                'id': paper.id,
                'source': paper.source,
                'external_id': paper.external_id,
                'title': paper.title,
                'abstract': paper.abstract,
                'url': paper.url,
                'pdf_url': paper.pdf_url,
                'published_date': paper.published_date,
                'journal': paper.journal,
                'doi': paper.doi,
                'data_types': paper.data_types,
                'extra_metadata': paper.extra_metadata
            }
            papers.append(paper_dict)

        return papers

    except Exception as e:
        logger.error(f"查询论文时出错: {e}")
        return []

    finally:
        session.close()


def get_repositories_by_criteria(start_date=None, end_date=None, days=None,
                                 languages=None, topics=None, limit=None):
    """
    根据条件获取代码仓库

    Args:
        start_date: 开始日期
        end_date: 结束日期
        days: 过去几天
        languages: 编程语言列表
        topics: 话题列表
        limit: 最大返回数量

    Returns:
        list: 代码仓库列表
    """
    session = Session()

    try:
        query = session.query(Repository)

        # 日期过滤
        if days is not None:
            start_date = datetime.now() - timedelta(days=days)
            query = query.filter(Repository.created_at >= start_date)
        elif start_date is not None and end_date is not None:
            query = query.filter(Repository.created_at.between(start_date, end_date))

        # 编程语言过滤
        if languages:
            lang_filters = []
            for lang in languages:
                lang_filters.append(Repository.language == lang)
            query = query.filter(or_(*lang_filters))

        # 话题过滤
        if topics:
            topic_filters = []
            for topic in topics:
                # 根据数据库类型选择合适的JSON查询方式
                if engine.name == 'sqlite':
                    # SQLite JSON查询
                    topic_filters.append(
                        func.json_extract(Repository.topics, '$').like(f'%{topic}%')
                    )
                elif engine.name == 'postgresql':
                    # PostgreSQL JSON查询
                    topic_filters.append(
                        Repository.topics.op('?')(topic)
                    )
                elif engine.name == 'mysql':
                    # MySQL JSON查询
                    topic_filters.append(
                        func.json_contains(Repository.topics, f'"{topic}"')
                    )
                else:
                    # 默认使用LIKE
                    topic_filters.append(
                        Repository.topics.cast(String).like(f'%{topic}%')
                    )

            if topic_filters:
                query = query.filter(or_(*topic_filters))

        # 排序
        query = query.order_by(Repository.stars.desc())

        # 限制返回数量
        if limit:
            query = query.limit(limit)

        # 执行查询并转换为字典
        repos = []
        for repo in query.all():
            repo_dict = {
                'id': repo.id,
                'owner': repo.owner,
                'name': repo.name,
                'full_name': repo.full_name,
                'url': repo.url,
                'description': repo.description,
                'stars': repo.stars,
                'forks': repo.forks,
                'watchers': repo.watchers,
                'language': repo.language,
                'created_at': repo.created_at,
                'updated_at': repo.updated_at,
                'topics': repo.topics,
                'extra_metadata': repo.extra_metadata
            }
            repos.append(repo_dict)

        return repos

    except Exception as e:
        logger.error(f"查询代码仓库时出错: {e}")
        return []

    finally:
        session.close()