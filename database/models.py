#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Float, Boolean, ForeignKey, Table, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

logger = logging.getLogger(__name__)

Base = declarative_base()

# 论文与作者的多对多关系表
paper_author = Table(
    'paper_author', Base.metadata,
    Column('paper_id', Integer, ForeignKey('papers.id')),
    Column('author_id', Integer, ForeignKey('authors.id'))
)

# 论文与关键词的多对多关系表
paper_keyword = Table(
    'paper_keyword', Base.metadata,
    Column('paper_id', Integer, ForeignKey('papers.id')),
    Column('keyword_id', Integer, ForeignKey('keywords.id'))
)

# 论文与主题的多对多关系表
paper_topic = Table(
    'paper_topic', Base.metadata,
    Column('paper_id', Integer, ForeignKey('papers.id')),
    Column('topic_id', Integer, ForeignKey('topics.id'))
)

# 论文与数据集的多对多关系表
paper_dataset = Table(
    'paper_dataset', Base.metadata,
    Column('paper_id', Integer, ForeignKey('papers.id')),
    Column('dataset_id', Integer, ForeignKey('datasets.id'))
)

# 论文与GitHub仓库的多对多关系表
paper_repository = Table(
    'paper_repository', Base.metadata,
    Column('paper_id', Integer, ForeignKey('papers.id')),
    Column('repository_id', Integer, ForeignKey('repositories.id'))
)


class Paper(Base):
    """论文模型"""
    __tablename__ = 'papers'

    id = Column(Integer, primary_key=True)
    source = Column(String(50), nullable=False)  # arxiv, biorxiv, nature, science, cell
    external_id = Column(String(100), unique=True, index=True)
    title = Column(String(500), nullable=False)
    abstract = Column(Text)
    url = Column(String(500))
    pdf_url = Column(String(500))
    published_date = Column(DateTime, index=True)
    crawled_date = Column(DateTime, default=datetime.utcnow)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    journal = Column(String(200))
    volume = Column(String(50))
    issue = Column(String(50))
    doi = Column(String(100), index=True)

    # 数据类型标记 - 存储为JSON数组
    data_types = Column(JSON)

    # 额外元数据 - 存储为JSON (改名为extra_metadata避免冲突)
    extra_metadata = Column(JSON)

    # 关系
    authors = relationship('Author', secondary=paper_author, back_populates='papers')
    keywords = relationship('Keyword', secondary=paper_keyword, back_populates='papers')
    topics = relationship('Topic', secondary=paper_topic, back_populates='papers')
    datasets = relationship('Dataset', secondary=paper_dataset, back_populates='papers')
    repositories = relationship('Repository', secondary=paper_repository, back_populates='papers')


class Author(Base):
    """作者模型"""
    __tablename__ = 'authors'

    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False, index=True)
    email = Column(String(200))
    affiliation = Column(String(500))
    orcid = Column(String(50))  # ORCID标识符

    # 关系
    papers = relationship('Paper', secondary=paper_author, back_populates='authors')


class Keyword(Base):
    """关键词模型"""
    __tablename__ = 'keywords'

    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False, unique=True, index=True)

    # 关系
    papers = relationship('Paper', secondary=paper_keyword, back_populates='keywords')


class Topic(Base):
    """研究主题模型"""
    __tablename__ = 'topics'

    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False, unique=True, index=True)

    # 关系
    papers = relationship('Paper', secondary=paper_topic, back_populates='topics')


class Dataset(Base):
    """数据集模型"""
    __tablename__ = 'datasets'

    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    description = Column(Text)
    url = Column(String(500), index=True)
    doi = Column(String(100), index=True)
    source = Column(String(50))  # 数据集来源: paper_mention, direct_search, etc.
    platform = Column(String(50))  # 托管平台: figshare, zenodo, etc.
    size = Column(String(50))  # 数据集大小
    format = Column(String(50))  # 数据格式
    license = Column(String(100))  # 许可证
    accession = Column(String(100), index=True)  # 访问号码(如GEO, SRA等)
    crawled_date = Column(DateTime, default=datetime.utcnow)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # 数据类型 - 存储为JSON数组
    data_types = Column(JSON)

    # 额外元数据 - 存储为JSON (改名为extra_metadata避免冲突)
    extra_metadata = Column(JSON)

    # 是否已验证
    verified = Column(Boolean, default=False)

    # 关系
    papers = relationship('Paper', secondary=paper_dataset, back_populates='datasets')


class Repository(Base):
    """GitHub仓库模型"""
    __tablename__ = 'repositories'

    id = Column(Integer, primary_key=True)
    owner = Column(String(100), nullable=False)
    name = Column(String(200), nullable=False)
    full_name = Column(String(300), nullable=False, unique=True, index=True)
    url = Column(String(500))
    description = Column(Text)
    stars = Column(Integer, default=0)
    forks = Column(Integer, default=0)
    watchers = Column(Integer, default=0)
    language = Column(String(50))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    last_commit = Column(String(100))  # 最后一次提交的SHA
    last_commit_date = Column(DateTime)
    has_readme = Column(Boolean)
    has_license = Column(Boolean)

    # 主题标签 - 存储为JSON数组
    topics = Column(JSON)

    # 额外元数据 - 存储为JSON (改名为extra_metadata避免冲突)
    extra_metadata = Column(JSON)

    # 关系
    papers = relationship('Paper', secondary=paper_repository, back_populates='repositories')


class CrawlLog(Base):
    """爬取日志模型，记录每次爬取的状态"""
    __tablename__ = 'crawl_logs'

    id = Column(Integer, primary_key=True)
    source = Column(String(50), nullable=False)  # 数据来源
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime)
    status = Column(String(20))  # success, failure, partial
    papers_count = Column(Integer, default=0)
    datasets_count = Column(Integer, default=0)
    error_message = Column(Text)

    # 额外信息 - 存储为JSON
    details = Column(JSON)