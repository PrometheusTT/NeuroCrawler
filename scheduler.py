#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import time
import threading
import signal
import sys
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor

logger = logging.getLogger(__name__)


class Scheduler:
    """
    任务调度器，管理周期性任务执行
    支持间隔执行、定时执行和Cron表达式
    """

    def __init__(self, config=None):
        """
        初始化调度器

        Args:
            config: 调度器配置字典
        """
        self.config = config or {}
        self.running = False
        self.scheduler = None
        self.jobs = []

        # 读取配置
        self.thread_pool_size = self.config.get('thread_pool_size', 10)
        self.job_max_instances = self.config.get('job_max_instances', 1)
        self.timezone = self.config.get('timezone', 'UTC')

        # 注册信号处理器
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _initialize_scheduler(self):
        """初始化APScheduler"""
        if self.scheduler is not None:
            return

        try:
            # 创建调度器
            jobstores = {
                'default': MemoryJobStore()
            }

            executors = {
                'default': ThreadPoolExecutor(self.thread_pool_size)
            }

            job_defaults = {
                'coalesce': True,  # 合并延迟的任务
                'max_instances': self.job_max_instances,  # 同一任务的最大实例数
                'misfire_grace_time': 60 * 60  # 错过执行时间的容忍度（秒）
            }

            self.scheduler = BackgroundScheduler(
                jobstores=jobstores,
                executors=executors,
                job_defaults=job_defaults,
                timezone=self.timezone
            )

            logger.info("调度器初始化成功")

        except Exception as e:
            logger.error(f"初始化调度器失败: {e}")
            raise

    def add_job(self, func, trigger=None, trigger_args=None, job_id=None, name=None, **kwargs):
        """
        添加任务

        Args:
            func: 要执行的函数
            trigger: 触发器类型 ('interval', 'cron', 'date', 或直接传入触发器实例)
            trigger_args: 触发器参数字典
            job_id: 任务ID
            name: 任务名称
            **kwargs: 传递给func的参数

        Returns:
            job: 已添加的任务
        """
        self._initialize_scheduler()

        if trigger_args is None:
            trigger_args = {}

        job_id = job_id or f"job_{len(self.jobs) + 1}"
        name = name or f"job_{len(self.jobs) + 1}"

        try:
            # 如果未指定触发器，使用默认的每日触发器
            if trigger is None:
                # 默认每天凌晨2点执行
                default_hour = self.config.get('default_hour', 2)
                default_minute = self.config.get('default_minute', 0)
                trigger = 'cron'
                trigger_args = {'hour': default_hour, 'minute': default_minute}

            # 构建触发器
            if isinstance(trigger, str):
                if trigger == 'interval':
                    # 间隔触发器 - 默认每天一次
                    interval_seconds = trigger_args.get('seconds', 0)
                    interval_minutes = trigger_args.get('minutes', 0)
                    interval_hours = trigger_args.get('hours', 0)
                    interval_days = trigger_args.get('days', 1)

                    # 如果未指定任何间隔，默认为每天
                    if not any([interval_seconds, interval_minutes, interval_hours, interval_days]):
                        interval_days = 1

                    trigger_instance = IntervalTrigger(
                        seconds=interval_seconds,
                        minutes=interval_minutes,
                        hours=interval_hours,
                        days=interval_days
                    )

                elif trigger == 'cron':
                    # Cron触发器 - 使用cron表达式
                    year = trigger_args.get('year', None)
                    month = trigger_args.get('month', None)
                    day = trigger_args.get('day', None)
                    week = trigger_args.get('week', None)
                    day_of_week = trigger_args.get('day_of_week', None)
                    hour = trigger_args.get('hour', 0)
                    minute = trigger_args.get('minute', 0)
                    second = trigger_args.get('second', 0)

                    trigger_instance = CronTrigger(
                        year=year,
                        month=month,
                        day=day,
                        week=week,
                        day_of_week=day_of_week,
                        hour=hour,
                        minute=minute,
                        second=second
                    )

                elif trigger == 'date':
                    # 日期触发器 - 在特定日期/时间执行一次
                    from apscheduler.triggers.date import DateTrigger

                    run_date = trigger_args.get('run_date', datetime.now() + timedelta(seconds=10))
                    if isinstance(run_date, str):
                        run_date = datetime.fromisoformat(run_date.replace('Z', '+00:00'))

                    trigger_instance = DateTrigger(run_date=run_date)

                else:
                    raise ValueError(f"不支持的触发器类型: {trigger}")

            else:
                # 直接使用传入的触发器实例
                trigger_instance = trigger

            # 定义日志装饰器，用于记录任务执行情况
            def job_wrapper(*args, **job_kwargs):
                start_time = datetime.now()
                logger.info(f"任务开始执行: {name} (ID: {job_id})")

                try:
                    result = func(*args, **job_kwargs)
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    logger.info(f"任务执行成功: {name} (ID: {job_id}), 耗时: {duration:.2f}秒")
                    return result

                except Exception as e:
                    end_time = datetime.now()
                    duration = (end_time - start_time).total_seconds()
                    logger.error(f"任务执行失败: {name} (ID: {job_id}), 耗时: {duration:.2f}秒, 错误: {e}")
                    logger.exception(f"任务 {name} 异常详情:")

            # 添加任务
            job = self.scheduler.add_job(
                job_wrapper,
                trigger=trigger_instance,
                id=job_id,
                name=name,
                kwargs=kwargs,
                replace_existing=True
            )

            self.jobs.append(job)

            logger.info(f"成功添加任务: {name} (ID: {job_id})")

            # 打印下一次执行时间 - 使用安全的方式获取
            try:
                next_run = getattr(job, 'next_run_time', None)
                if next_run:
                    logger.info(f"任务 {name} 下一次执行时间: {next_run}")
            except Exception as e:
                # 忽略获取下一次执行时间的错误
                logger.debug(f"无法获取任务 {name} 的下一次执行时间: {e}")

            return job

        except Exception as e:
            logger.error(f"添加任务失败: {e}")
            raise

    def remove_job(self, job_id):
        """
        移除任务

        Args:
            job_id: 任务ID

        Returns:
            bool: 是否成功移除
        """
        if not self.scheduler:
            return False

        try:
            self.scheduler.remove_job(job_id)

            # 更新任务列表
            self.jobs = [job for job in self.jobs if job.id != job_id]

            logger.info(f"成功移除任务: {job_id}")
            return True

        except Exception as e:
            logger.error(f"移除任务 {job_id} 失败: {e}")
            return False

    def start(self):
        """启动调度器"""
        if self.running:
            logger.warning("调度器已经在运行")
            return

        try:
            self._initialize_scheduler()

            # 检查是否有任务
            if not self.jobs:
                logger.warning("调度器没有任务，但仍将启动")

            # 启动调度器
            self.scheduler.start()
            self.running = True

            logger.info("调度器已启动")

            # 打印所有任务的下一次执行时间
            # 打印所有任务的下一次执行时间
            for job in self.jobs:
                try:
                    next_run = getattr(job, 'next_run_time', None)
                    if next_run:
                        logger.info(f"任务 {job.name} 下一次执行时间: {next_run}")
                except Exception as e:
                    # 忽略获取下一次执行时间的错误
                    logger.debug(f"无法获取任务 {job.name} 的下一次执行时间")

            # 创建守护线程，避免主程序退出
            def keep_running():
                while self.running:
                    time.sleep(1)

            self.daemon_thread = threading.Thread(target=keep_running, daemon=True)
            self.daemon_thread.start()

            # 注册关闭前清理
            import atexit
            atexit.register(self.shutdown)

        except Exception as e:
            logger.error(f"启动调度器失败: {e}")
            raise

    def pause(self):
        """暂停调度器"""
        if not self.running or not self.scheduler:
            logger.warning("调度器未运行")
            return

        try:
            self.scheduler.pause()
            logger.info("调度器已暂停")

        except Exception as e:
            logger.error(f"暂停调度器失败: {e}")

    def resume(self):
        """恢复调度器"""
        if not self.scheduler:
            logger.warning("调度器未初始化")
            return

        try:
            self.scheduler.resume()
            logger.info("调度器已恢复")

        except Exception as e:
            logger.error(f"恢复调度器失败: {e}")

    def shutdown(self, wait=True):
        """
        关闭调度器

        Args:
            wait: 是否等待任务完成
        """
        if not self.running or not self.scheduler:
            return

        try:
            self.scheduler.shutdown(wait=wait)
            self.running = False
            logger.info("调度器已关闭")

        except Exception as e:
            logger.error(f"关闭调度器失败: {e}")

    def print_jobs(self):
        """打印所有任务信息"""
        if not self.scheduler:
            logger.info("调度器未初始化，没有任务")
            return

        try:
            jobs = self.scheduler.get_jobs()

            if not jobs:
                logger.info("调度器中没有任务")
                return

            logger.info(f"调度器中共有 {len(jobs)} 个任务:")

            for i, job in enumerate(jobs, 1):
                try:
                    next_run = getattr(job, 'next_run_time', None)
                    next_run_str = next_run.strftime('%Y-%m-%d %H:%M:%S') if next_run else "未调度"
                except:
                    next_run_str = "未知"

                logger.info(f"{i}. ID: {job.id}, 名称: {job.name}, 下一次执行: {next_run_str}")

        except Exception as e:
            logger.error(f"获取任务信息失败: {e}")

    def _signal_handler(self, sig, frame):
        """处理系统信号"""
        logger.info(f"接收到信号 {sig}，准备关闭调度器")
        self.shutdown()
        sys.exit(0)

    def add_daily_job(self, func, hour=2, minute=0, job_id=None, name=None, **kwargs):
        """
        添加每日任务（便捷方法）

        Args:
            func: 要执行的函数
            hour: 小时 (0-23)
            minute: 分钟 (0-59)
            job_id: 任务ID
            name: 任务名称
            **kwargs: 传递给func的参数

        Returns:
            job: 已添加的任务
        """
        trigger_args = {
            'hour': hour,
            'minute': minute
        }

        return self.add_job(
            func,
            trigger='cron',
            trigger_args=trigger_args,
            job_id=job_id,
            name=name or f"daily_job_{hour}_{minute}",
            **kwargs
        )

    def add_weekly_job(self, func, day_of_week=0, hour=2, minute=0, job_id=None, name=None, **kwargs):
        """
        添加每周任务（便捷方法）

        Args:
            func: 要执行的函数
            day_of_week: 星期几 (0-6 表示星期一到星期日)
            hour: 小时 (0-23)
            minute: 分钟 (0-59)
            job_id: 任务ID
            name: 任务名称
            **kwargs: 传递给func的参数

        Returns:
            job: 已添加的任务
        """
        trigger_args = {
            'day_of_week': day_of_week,
            'hour': hour,
            'minute': minute
        }

        return self.add_job(
            func,
            trigger='cron',
            trigger_args=trigger_args,
            job_id=job_id,
            name=name or f"weekly_job_{day_of_week}_{hour}_{minute}",
            **kwargs
        )

    def add_interval_job(self, func, hours=0, minutes=0, seconds=0, job_id=None, name=None, **kwargs):
        """
        添加间隔任务（便捷方法）

        Args:
            func: 要执行的函数
            hours: 小时间隔
            minutes: 分钟间隔
            seconds: 秒间隔
            job_id: 任务ID
            name: 任务名称
            **kwargs: 传递给func的参数

        Returns:
            job: 已添加的任务
        """
        # 确保至少有一个时间单位被设置
        if hours == 0 and minutes == 0 and seconds == 0:
            hours = 1  # 默认1小时

        trigger_args = {
            'hours': hours,
            'minutes': minutes,
            'seconds': seconds
        }

        interval_desc = ""
        if hours > 0:
            interval_desc += f"{hours}h"
        if minutes > 0:
            interval_desc += f"{minutes}m"
        if seconds > 0:
            interval_desc += f"{seconds}s"

        return self.add_job(
            func,
            trigger='interval',
            trigger_args=trigger_args,
            job_id=job_id,
            name=name or f"interval_job_{interval_desc}",
            **kwargs
        )


if __name__ == "__main__":
    # 示例用法
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


    def test_job():
        print(f"测试任务执行时间: {datetime.now()}")


    # 创建调度器
    scheduler = Scheduler()

    # 添加每分钟执行一次的任务
    scheduler.add_interval_job(test_job, minutes=1, job_id="test_job_1", name="每分钟测试任务")

    # 添加每天特定时间执行的任务
    current_time = datetime.now()
    test_hour = current_time.hour
    test_minute = (current_time.minute + 2) % 60  # 2分钟后

    scheduler.add_daily_job(test_job, hour=test_hour, minute=test_minute,
                            job_id="test_job_2", name="每日测试任务")

    # 打印任务信息
    scheduler.print_jobs()

    # 启动调度器
    scheduler.start()

    try:
        # 保持主线程运行
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("接收到终止信号，关闭调度器")
        scheduler.shutdown()