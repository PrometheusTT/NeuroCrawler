#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import yaml
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def load_config(config_path='config.yaml'):
    """
    加载配置文件

    Args:
        config_path (str): 配置文件路径

    Returns:
        dict: 配置字典
    """
    default_config = {
        'database': {
            'type': 'sqlite',
            'path': 'neurocrawler.db'
        },
        'logging': {
            'level': 'INFO',
            'file': 'neurocrawler.log'
        },
        'sources': {
            'nature': {
                'enabled': True,
                'journals': ['nature', 'nature-neuroscience', 'nature-methods', 'nature-communications'],
                'browser_emulation': True,
                'days_to_crawl': 30
            },
            'science': {
                'enabled': True,
                'journals': ['science', 'science-advances', 'science-translational-medicine'],
                'browser_emulation': True,
                'days_to_crawl': 30
            },
            'cell': {
                'enabled': True,
                'journals': ['cell', 'neuron', 'cell-reports'],
                'browser_emulation': True,
                'days_to_crawl': 30
            }
        },
        'proxy': {
            'enabled': False,
            'update_interval_minutes': 30,
            'proxy_list': []
        },
        'extraction': {
            'dataset_types': [
                'neuron_imaging', 'reconstruction', 'spatial_transcriptomics',
                'mri', 'electrophysiology', 'behavioral', 'histology'
            ]
        },
        'output': {
            'save_html': False,
            'html_dir': 'html_cache'
        }
    }

    # 如果配置文件存在，加载它
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                user_config = yaml.safe_load(f)

            # 将用户配置与默认配置合并
            if user_config:
                merged_config = _merge_configs(default_config, user_config)
            else:
                merged_config = default_config

            logger.info(f"已加载配置文件: {config_path}")
            return merged_config

        except Exception as e:
            logger.error(f"加载配置文件失败: {e}")
            logger.info("使用默认配置")
            return default_config
    else:
        # 配置文件不存在，创建一个默认配置文件
        try:
            with open(config_path, 'w') as f:
                yaml.dump(default_config, f, default_flow_style=False)
            logger.info(f"已创建默认配置文件: {config_path}")
        except Exception as e:
            logger.error(f"创建默认配置文件失败: {e}")

        return default_config


def _merge_configs(default_config, user_config):
    """递归合并配置字典"""
    merged = default_config.copy()

    for key, value in user_config.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _merge_configs(merged[key], value)
        else:
            merged[key] = value

    return merged


def get_run_info():
    """获取运行信息"""
    return {
        'start_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'version': '1.0.0'
    }