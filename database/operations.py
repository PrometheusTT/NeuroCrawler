#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import os
import shutil
from datetime import datetime, timedelta
from sqlalchemy import create_engine, func, desc, or_, and_
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import SQLAlchemyError
from database.models import Base, Paper, Author, Keyword, Topic, Dataset, Repository

logger = logging.getLogger(__name__)

# 全局数据库会话工厂
Session = None
engine = None


def initialize_db(db_config):
    """初始化数据库连接和表结构"""
    global Session, engine

    try:
        # 数据库配置
        db_type = db_config.get('type', 'sqlite')

        if db_type == 'sqlite':
            db_path = db_config.get('path', 'neurocrawler.db')
            # 确保目录存在
            db_dir = os.path.dirname(os.path.abspath(db_path))
            os.makedirs(db_dir, exist_ok=True)

            # 创建备份（如果需要）
            if os.path.exists(db_path):
                last_backup_file = f"{db_path}.backup"
                backup_interval = db_config.get('backup_interval', 86400)  # 默认1天

                # 检查是否需要备份
                if not os.path.exists(last_backup_file) or \
                        (datetime.now().timestamp() - os.path.getmtime(last_backup_file)) > backup_interval:
                    backup_path = f"{db_path}.backup.{datetime.now().strftime('%Y%m%d%H%M%S')}"
                    shutil.copy2(db_path, backup_path)
                    # 创建或更新最新备份的软链接
                    if os.path.exists(last_backup_file):
                        os.remove(last_backup_file)
                    shutil.copy2(db_path, last_backup_file)
                    logger.info(f"数据库已备份到 {backup_path}")

            # 创建数据库连接
            db_url = f"sqlite:///{db_path}"
            engine = create_engine(db_url)

        elif db_type == 'mysql':
            # MySQL配置
            host = db_config.get('host', 'localhost')
            port = db_config.get('port', 3306)
            database = db_config.get('database', 'neurocrawler')
            user = db_config.get('user', 'root')
            password = db_config.get('password', '')

            db_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4"
            engine = create_engine(db_url, pool_size=10, max_overflow=20)

        elif db_type == 'postgresql':
            # PostgreSQL配置
            host = db_config.get('host', 'localhost')
            port = db_config.get('port', 5432)
            database = db_config.get('database', 'neurocrawler')
            user = db_config.get('user', 'postgres')
            password = db_config.get('password', '')

            db_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
            engine = create_engine(db_url, pool_size=10, max_overflow=20)

        else:
            raise ValueError(f"不支持的数据库类型: {db_type}")

        # 创建表结构
        Base.metadata.create_all(engine)

        # 创建会话工厂
        Session = scoped_session(sessionmaker(bind=engine))

        logger.info(f"数据库初始化完成: {db_url}")
        return True

    except Exception as e:
        logger.error(f"初始化数据库时出错: {e}")
        raise


def save_papers(papers):
    """
    保存论文数据到数据库
    处理论文与作者、关键词、主题等的关联关系
    """
    if not Session:
        logger.error("数据库未初始化，请先调用initialize_db()")
        return False

    session = Session()
    try:
        saved_count = 0

        for paper_data in papers:
            try:
                # 检查论文是否已存在
                external_id = paper_data.get('external_id') or paper_data.get('doi') or paper_data.get('id')
                if not external_id:
                    logger.warning(f"论文缺少external_id或doi: {paper_data.get('title')}")
                    continue

                existing_paper = session.query(Paper).filter(
                    (Paper.external_id == external_id) |
                    (Paper.doi == paper_data.get('doi'))
                ).first()

                if existing_paper:
                    # 更新现有论文
                    for key, value in paper_data.items():
                        if key not in ['id', 'authors', 'keywords', 'topics', 'datasets', 'repositories']:
                            setattr(existing_paper, key, value)

                    paper = existing_paper
                    logger.debug(f"更新现有论文: {paper.title}")
                else:
                    # 创建新论文
                    paper = Paper(
                        source=paper_data.get('source'),
                        external_id=external_id,
                        title=paper_data.get('title'),
                        abstract=paper_data.get('abstract'),
                        url=paper_data.get('url'),
                        pdf_url=paper_data.get('pdf_url'),
                        published_date=paper_data.get('published_date'),
                        journal=paper_data.get('journal'),
                        volume=paper_data.get('volume'),
                        issue=paper_data.get('issue'),
                        doi=paper_data.get('doi')
                    )
                    session.add(paper)
                    logger.debug(f"添加新论文: {paper.title}")

                # 处理作者
                if 'authors' in paper_data and paper_data['authors']:
                    # 清除现有作者关联
                    paper.authors = []

                    for author_name in paper_data['authors']:
                        # 查找或创建作者
                        author = session.query(Author).filter(Author.name == author_name).first()
                        if not author:
                            author = Author(name=author_name)
                            session.add(author)

                        # 添加到论文的作者列表
                        paper.authors.append(author)

                # 处理关键词
                if 'keywords' in paper_data and paper_data['keywords']:
                    # 清除现有关键词关联
                    paper.keywords = []

                    for keyword_name in paper_data['keywords']:
                        # 查找或创建关键词
                        keyword = session.query(Keyword).filter(Keyword.name == keyword_name).first()
                        if not keyword:
                            keyword = Keyword(name=keyword_name)
                            session.add(keyword)

                        # 添加到论文的关键词列表
                        paper.keywords.append(keyword)

                # 处理研究主题
                if 'topics' in paper_data and paper_data['topics']:
                    # 清除现有主题关联
                    paper.topics = []

                    for topic_name in paper_data['topics']:
                        # 查找或创建主题
                        topic = session.query(Topic).filter(Topic.name == topic_name).first()
                        if not topic:
                            topic = Topic(name=topic_name)
                            session.add(topic)

                        # 添加到论文的主题列表
                        paper.topics.append(topic)

                # 先提交以获取论文ID
                session.commit()
                saved_count += 1

            except Exception as e:
                logger.error(f"保存论文时出错: {e}, 论文标题: {paper_data.get('title')}")
                session.rollback()
                continue

        logger.info(f"成功保存了 {saved_count} 篇论文")
        return True
    except Exception as e:
        logger.error(f"保存论文时出错: {e}")
        session.rollback()
        return False
    finally:
        session.close()


def save_datasets(datasets):
    """
    保存数据集信息到数据库
    处理数据集与论文的关联关系
    """
    if not Session:
        logger.error("数据库未初始化，请先调用initialize_db()")
        return False

    session = Session()
    try:
        saved_count = 0

        for dataset_data in datasets:
            try:
                # 确定唯一标识符
                dataset_url = dataset_data.get('url')
                dataset_name = dataset_data.get('name')
                dataset_doi = dataset_data.get('doi')

                # 检查数据集是否已存在
                existing_dataset = None
                if dataset_url:
                    existing_dataset = session.query(Dataset).filter(Dataset.url == dataset_url).first()

                if not existing_dataset and dataset_doi:
                    existing_dataset = session.query(Dataset).filter(Dataset.doi == dataset_doi).first()

                if not existing_dataset and dataset_name and dataset_data.get('paper_id'):
                    # 同一篇论文中同名数据集视为相同
                    existing_dataset = session.query(Dataset).join(
                        Dataset.papers
                    ).filter(
                        Dataset.name == dataset_name,
                        Paper.external_id == dataset_data.get('paper_id')
                    ).first()

                if existing_dataset:
                    # 更新现有数据集
                    for key, value in dataset_data.items():
                        if key not in ['id', 'papers']:
                            setattr(existing_dataset, key, value)

                    dataset = existing_dataset
                    logger.debug(f"更新现有数据集: {dataset.name}")
                else:
                    # 创建新数据集
                    dataset = Dataset(
                        name=dataset_name,
                        description=dataset_data.get('description'),
                        url=dataset_url,
                        doi=dataset_doi,
                        source=dataset_data.get('source'),
                        platform=dataset_data.get('repository'),
                        size=dataset_data.get('size'),
                        format=dataset_data.get('format'),
                        license=dataset_data.get('license')
                    )
                    session.add(dataset)
                    logger.debug(f"添加新数据集: {dataset.name}")

                # 关联到论文
                if 'paper_id' in dataset_data:
                    paper = session.query(Paper).filter(
                        (Paper.external_id == dataset_data['paper_id']) |
                        (Paper.doi == dataset_data['paper_id'])
                    ).first()

                    if paper and paper not in dataset.papers:
                        dataset.papers.append(paper)

                # 提交以保存数据集
                session.commit()
                saved_count += 1

            except Exception as e:
                logger.error(f"保存数据集时出错: {e}, 数据集: {dataset_data.get('name')}")
                session.rollback()
                continue

        logger.info(f"成功保存了 {saved_count} 个数据集")
        return True
    except Exception as e:
        logger.error(f"保存数据集时出错: {e}")
        session.rollback()
        return False
    finally:
        session.close()


def save_repositories(repositories):
    """
    保存GitHub仓库信息到数据库
    处理仓库与论文的关联关系
    """
    if not Session:
        logger.error("数据库未初始化，请先调用initialize_db()")
        return False

    session = Session()
    try:
        saved_count = 0

        for repo_data in repositories:
            try:
                # 检查仓库是否已存在
                full_name = repo_data.get('full_name')
                if not full_name and repo_data.get('user') and repo_data.get('repo'):
                    full_name = f"{repo_data['user']}/{repo_data['repo']}"

                if not full_name:
                    logger.warning(f"仓库缺少full_name: {repo_data}")
                    continue

                existing_repo = session.query(Repository).filter(Repository.full_name == full_name).first()

                if existing_repo:
                    # 更新现有仓库
                    for key, value in repo_data.items():
                        if key not in ['id', 'papers']:
                            setattr(existing_repo, key, value)

                    repo = existing_repo
                    logger.debug(f"更新现有仓库: {repo.full_name}")
                else:
                    # 创建新仓库
                    repo = Repository(
                        owner=repo_data.get('user'),
                        name=repo_data.get('repo'),
                        full_name=full_name,
                        url=repo_data.get('url'),
                        description=repo_data.get('description'),
                        stars=repo_data.get('stars', 0),
                        forks=repo_data.get('forks', 0)
                    )
                    session.add(repo)
                    logger.debug(f"添加新仓库: {repo.full_name}")

                # 关联到论文
                if 'referenced_in' in repo_data and repo_data['referenced_in'].get('paper_id'):
                    paper_id = repo_data['referenced_in']['paper_id']
                    paper = session.query(Paper).filter(
                        (Paper.external_id == paper_id) |
                        (Paper.doi == paper_id)
                    ).first()

                    if paper and paper not in repo.papers:
                        repo.papers.append(paper)

                # 提交以保存仓库
                session.commit()
                saved_count += 1

            except Exception as e:
                logger.error(f"保存仓库时出错: {e}, 仓库: {repo_data.get('full_name')}")
                session.rollback()
                continue

        logger.info(f"成功保存了 {saved_count} 个仓库")
        return True
    except Exception as e:
        logger.error(f"保存仓库时出错: {e}")
        session.rollback()
        return False
    finally:
        session.close()


def get_recent_papers(days=7, data_types=None, limit=50):
    """
    获取最近的论文，可按数据类型过滤

    参数:
        days: 过去几天的数据
        data_types: 数据类型列表，如 ['neuron_imaging', 'mri']
        limit: 返回结果数量限制

    返回:
        论文列表
    """
    if not Session:
        logger.error("数据库未初始化，请先调用initialize_db()")
        return []

    session = Session()
    try:
        query = session.query(Paper)

        # 时间过滤
        if days:
            start_date = datetime.now() - timedelta(days=days)
            query = query.filter(Paper.published_date >= start_date)

        # 数据类型过滤
        if data_types:
            # 查找包含指定数据类型的数据集
            query = query.join(Paper.datasets).filter(
                or_(*[Dataset.data_types.like(f"%{dtype}%") for dtype in data_types])
            )

        # 按发布时间倒序
        query = query.order_by(desc(Paper.published_date))

        # 限制结果数量
        if limit:
            query = query.limit(limit)

        # 获取结果
        papers = query.all()

        # 转换为字典列表
        results = []
        for paper in papers:
            # 基本信息
            paper_dict = {
                'id': paper.id,
                'external_id': paper.external_id,
                'title': paper.title,
                'abstract': paper.abstract,
                'url': paper.url,
                'pdf_url': paper.pdf_url,
                'published_date': paper.published_date.isoformat() if paper.published_date else None,
                'journal': paper.journal,
                'source': paper.source,
                'doi': paper.doi,
                'authors': [author.name for author in paper.authors],
                'datasets': []
            }

            # 添加数据集信息
            for dataset in paper.datasets:
                dataset_dict = {
                    'id': dataset.id,
                    'name': dataset.name,
                    'url': dataset.url,
                    'repository': dataset.platform,
                    'data_types': dataset.data_types
                }
                paper_dict['datasets'].append(dataset_dict)

            results.append(paper_dict)

        return results

    except Exception as e:
        logger.error(f"获取最近论文时出错: {e}")
        return []
    finally:
        session.close()


def get_datasets_by_type(data_type, limit=50):
    """
    按数据类型获取数据集

    参数:
        data_type: 数据类型，如 'neuron_imaging'
        limit: 返回结果数量限制

    返回:
        数据集列表
    """
    if not Session:
        logger.error("数据库未初始化，请先调用initialize_db()")
        return []

    session = Session()
    try:
        query = session.query(Dataset)

        # 数据类型过滤
        query = query.filter(Dataset.data_types.like(f"%{data_type}%"))

        # 按最后更新时间倒序
        query = query.order_by(desc(Dataset.last_updated))

        # 限制结果数量
        if limit:
            query = query.limit(limit)

        # 获取结果
        datasets = query.all()

        # 转换为字典列表
        results = []
        for dataset in datasets:
            # 基本信息
            dataset_dict = {
                'id': dataset.id,
                'name': dataset.name,
                'description': dataset.description,
                'url': dataset.url,
                'repository': dataset.platform,
                'doi': dataset.doi,
                'source': dataset.source,
                'data_types': dataset.data_types,
                'papers': []
            }

            # 添加相关论文
            for paper in dataset.papers:
                paper_dict = {
                    'id': paper.id,
                    'title': paper.title,
                    'url': paper.url,
                    'doi': paper.doi,
                    'journal': paper.journal
                }
                dataset_dict['papers'].append(paper_dict)

            results.append(dataset_dict)

        return results

    except Exception as e:
        logger.error(f"按数据类型获取数据集时出错: {e}")
        return []
    finally:
        session.close()


def get_data_statistics():
    """
    获取数据统计信息

    返回:
        统计信息字典
    """
    if not Session:
        logger.error("数据库未初始化，请先调用initialize_db()")
        return {}

    session = Session()
    try:
        stats = {}

        # 论文总数
        stats['total_papers'] = session.query(func.count(Paper.id)).scalar()

        # 数据集总数
        stats['total_datasets'] = session.query(func.count(Dataset.id)).scalar()

        # 仓库总数
        stats['total_repositories'] = session.query(func.count(Repository.id)).scalar()

        # 按来源统计论文
        source_counts = session.query(
            Paper.source, func.count(Paper.id)
        ).group_by(Paper.source).all()

        stats['papers_by_source'] = {source: count for source, count in source_counts}

        # 按数据类型统计数据集
        stats['datasets_by_type'] = {}

        # 数据类型列表
        data_types = ['neuron_imaging', 'reconstruction', 'spatial_transcriptomics', 'mri', 'electrophysiology']

        for data_type in data_types:
            count = session.query(func.count(Dataset.id)).filter(
                Dataset.data_types.like(f"%{data_type}%")
            ).scalar()
            stats['datasets_by_type'][data_type] = count

        # 按仓库平台统计数据集
        platform_counts = session.query(
            Dataset.platform, func.count(Dataset.id)
        ).group_by(Dataset.platform).all()

        stats['datasets_by_platform'] = {platform if platform else 'unknown': count for platform, count in
                                         platform_counts}

        # 过去7天的论文数量
        week_ago = datetime.now() - timedelta(days=7)
        stats['papers_last_week'] = session.query(func.count(Paper.id)).filter(
            Paper.published_date >= week_ago
        ).scalar()

        return stats

    except Exception as e:
        logger.error(f"获取统计信息时出错: {e}")
        return {}
    finally:
        session.close()


def search_papers(query, limit=50):
    """
    搜索论文

    参数:
        query: 搜索关键词
        limit: 返回结果数量限制

    返回:
        论文列表
    """
    if not Session:
        logger.error("数据库未初始化，请先调用initialize_db()")
        return []

    session = Session()
    try:
        # 构建搜索条件
        search = f"%{query}%"
        papers = session.query(Paper).filter(
            or_(
                Paper.title.like(search),
                Paper.abstract.like(search),
                Paper.doi.like(search)
            )
        ).order_by(desc(Paper.published_date)).limit(limit).all()

        # 转换为字典列表
        results = []
        for paper in papers:
            paper_dict = {
                'id': paper.id,
                'title': paper.title,
                'abstract': paper.abstract[:200] + '...' if paper.abstract and len(
                    paper.abstract) > 200 else paper.abstract,
                'url': paper.url,
                'published_date': paper.published_date.isoformat() if paper.published_date else None,
                'journal': paper.journal,
                'source': paper.source,
                'doi': paper.doi,
                'authors': [author.name for author in paper.authors],
                'datasets_count': len(paper.datasets)
            }
            results.append(paper_dict)

        return results

    except Exception as e:
        logger.error(f"搜索论文时出错: {e}")
        return []
    finally:
        session.close()