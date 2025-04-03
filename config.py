#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import yaml
import json
import logging

logger = logging.getLogger(__name__)


def load_config(config_file='config.yaml'):
    """
    加载配置文件
    支持YAML和JSON格式
    """
    try:
        if not os.path.exists(config_file):
            logger.warning(f"配置文件 {config_file} 不存在，使用默认配置")
            return get_default_config()

        _, ext = os.path.splitext(config_file)

        if ext.lower() in ['.yml', '.yaml']:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
        elif ext.lower() == '.json':
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
        else:
            logger.warning(f"不支持的配置文件格式: {ext}，使用默认配置")
            return get_default_config()

        # 合并默认配置
        default_config = get_default_config()
        merged_config = merge_configs(default_config, config)

        return merged_config

    except Exception as e:
        logger.error(f"加载配置文件时出错: {e}")
        return get_default_config()


def get_default_config():
    """获取默认配置"""
    return {
        'sources': {
            'arxiv': {
                'enabled': True,
                'categories': ['q-bio.NC', 'q-bio.QM', 'stat.ML', 'cs.LG', 'cs.AI', 'cs.CV', 'cs.NE'],
                'days_back': 30,
                'max_results': 100
            },
            'biorxiv': {
                'enabled': True,
                'categories': ['neuroscience', 'bioinformatics', 'computational-biology'],
                'days_back': 30,
                'max_results': 100
            },
            'nature': {
                'enabled': True,
                'journals': [
                    'nature', 'nature-neuroscience', 'nature-methods',
                    'nature-communications', 'scientific-reports', 'nature-machine-intelligence'
                ],
                'days_back': 30,
                'max_results': 50
            },
            'science': {
                'enabled': True,
                'journals': [
                    'science', 'science-advances', 'science-translational-medicine',
                    'science-signaling'
                ],
                'days_back': 30,
                'max_results': 50
            },
            'cell': {
                'enabled': True,
                'journals': [
                    'cell', 'neuron', 'cell-reports', 'current-biology',
                    'cell-systems', 'patterns'
                ],
                'days_back': 30,
                'max_results': 50
            },
            'github': {
                'enabled': True,
                'api_tokens': [],
                'search_keywords': [
                    'neuroscience', 'neural-network', 'brain-model', 'neuroimaging',
                    'connectome', 'spike-sorting', 'eeg-analysis', 'fmri-analysis'
                ],
                'max_repos_per_search': 30
            }
        },
        'database': {
            'type': 'sqlite',
            'path': 'neurocrawler.db',
            'backup_interval': 86400  # 每天备份一次
        },
        'scheduler': {
            'enabled': True,
            'interval': 86400,  # 每天执行一次
            'start_time': '01:00',  # 凌晨1点开始执行
            'timezone': 'UTC'
        },
        'proxy': {
            'enabled': False,
            'update_interval': 3600,
            'test_url': 'https://www.google.com',
            'sources': {
                'free_proxy_list': {
                    'enabled': True,
                    'url': 'https://free-proxy-list.net/'
                },
                'custom_list': {
                    'enabled': False,
                    'proxies': []
                }
            }
        },
        'notification': {
            'enabled': False,
            'methods': {
                'email': {
                    'enabled': False,
                    'smtp_server': 'smtp.gmail.com',
                    'smtp_port': 587,
                    'sender': '',
                    'password': '',
                    'recipients': []
                },
                'slack': {
                    'enabled': False,
                    'webhook_url': ''
                },
                'telegram': {
                    'enabled': False,
                    'token': '',
                    'chat_id': ''
                }
            }
        },
        'logging': {
            'level': 'INFO',
            'file': 'neurocrawler.log',
            'max_size': 10485760,  # 10MB
            'backup_count': 5
        }
    }


def merge_configs(default_config, user_config):
    """合并默认配置和用户配置"""
    if not user_config:
        return default_config

    merged = default_config.copy()

    for key, value in user_config.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = merge_configs(merged[key], value)
        else:
            merged[key] = value

    return merged


def generate_sample_config(output_file='config.sample.yaml'):
    """生成样例配置文件"""
    try:
        config = get_default_config()

        with open(output_file, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)

        logger.info(f"样例配置文件已生成: {output_file}")

    except Exception as e:
        logger.error(f"生成样例配置文件时出错: {e}")