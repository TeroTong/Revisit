"""
定时任务调度器

负责：
1. 每日生日回访任务
2. 数据同步任务
3. 数据库备份任务
4. 健康检查任务
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from config.settings import settings

logger = logging.getLogger(__name__)


class SchedulerManager:
    """定时任务调度器管理"""

    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.is_running = False
        self._data_service = None

    async def _get_data_service(self):
        """获取数据服务（延迟加载）"""
        if self._data_service is None:
            from services.data_sync import DataSyncService
            self._data_service = DataSyncService()
            await self._data_service.init_connections()
        return self._data_service

    def start(self):
        """启动调度器"""
        if self.is_running:
            logger.warning("调度器已经在运行")
            return

        logger.info("正在启动定时任务调度器...")

        try:
            # 添加任务
            self._add_jobs()

            # 启动调度器
            self.scheduler.start()
            self.is_running = True

            logger.info("✅ 定时任务调度器启动成功")

        except Exception as e:
            logger.error(f"启动定时任务调度器失败: {e}")
            raise

    def _add_jobs(self):
        """添加任务"""

        # 每日回访任务（每天上午9点）
        self.scheduler.add_job(
            self._run_daily_reminders,
            CronTrigger(hour=9, minute=0),
            id='daily_reminders',
            name='每日回访任务',
            replace_existing=True
        )

        # 数据导入检查任务（每小时检查增量目录）
        self.scheduler.add_job(
            self._check_incremental_import,
            IntervalTrigger(hours=1),
            id='incremental_import',
            name='增量数据导入检查',
            replace_existing=True
        )

        # 健康检查任务（每30分钟）
        self.scheduler.add_job(
            self._health_check,
            IntervalTrigger(minutes=30),
            id='health_check',
            name='健康检查任务',
            replace_existing=True
        )

        # 统计报告任务（每天早上8点）
        self.scheduler.add_job(
            self._generate_daily_report,
            CronTrigger(hour=8, minute=0),
            id='daily_report',
            name='每日统计报告',
            replace_existing=True
        )

        logger.info(f"添加了 {len(self.scheduler.get_jobs())} 个定时任务")

    async def _run_daily_reminders(self):
        """运行每日回访任务"""
        logger.info("开始执行每日回访任务...")

        try:
            from services.birthday_reminder import BirthdayReminderService
            service = BirthdayReminderService()

            for institution_code in settings.APP.INSTITUTIONS:
                try:
                    logger.info(f"处理机构 {institution_code} 的回访任务...")
                    await service.run_daily_reminders(institution_code)
                except Exception as e:
                    logger.error(f"处理机构 {institution_code} 的回访任务失败: {e}")
                    continue

            logger.info("✅ 每日回访任务执行完成")

        except Exception as e:
            logger.error(f"执行每日回访任务失败: {e}")

    async def _check_incremental_import(self):
        """检查并处理增量数据导入"""
        logger.info("检查增量数据导入...")

        try:
            from pathlib import Path
            pending_dir = Path(settings.APP.DATA_DIR) / "import" / "incremental" / "pending"

            if not pending_dir.exists():
                logger.debug("增量目录不存在")
                return

            # 检查是否有待处理的数据
            pending_batches = [d for d in pending_dir.iterdir() if d.is_dir()]

            if pending_batches:
                logger.info(f"发现 {len(pending_batches)} 个待处理的增量批次")

                # 调用导入脚本
                from scripts.import_data import DataImporter
                importer = DataImporter()
                await importer.init()

                try:
                    await importer.process_incremental()
                finally:
                    await importer.close()

                logger.info("✅ 增量数据导入完成")
            else:
                logger.debug("没有待处理的增量数据")

        except Exception as e:
            logger.error(f"检查增量数据导入失败: {e}")

    async def _health_check(self):
        """健康检查任务"""
        logger.info("开始执行健康检查任务...")

        try:
            checks = []

            # 检查 PostgreSQL
            try:
                from database.postgres.connection import PostgreSQLConnection
                async with PostgreSQLConnection.get_connection() as conn:
                    await conn.fetchval("SELECT 1")
                    checks.append(("PostgreSQL", "✅", "连接正常"))
            except Exception as e:
                checks.append(("PostgreSQL", "❌", str(e)[:50]))

            # 检查 NebulaGraph
            try:
                from database.nebula.connection import test_nebula_connection
                if test_nebula_connection():
                    checks.append(("NebulaGraph", "✅", "连接正常"))
                else:
                    checks.append(("NebulaGraph", "⚠️", "连接失败"))
            except Exception as e:
                checks.append(("NebulaGraph", "❌", str(e)[:50]))

            # 检查 Qdrant
            try:
                from database.qdrant.connection import QdrantConnection
                qdrant = QdrantConnection()
                client = qdrant.get_client()
                if client:
                    collections = client.get_collections()
                    checks.append(("Qdrant", "✅", f"{len(collections.collections)} 个集合"))
                else:
                    checks.append(("Qdrant", "⚠️", "客户端未初始化"))
            except Exception as e:
                checks.append(("Qdrant", "❌", str(e)[:50]))

            # 检查 ClickHouse
            try:
                from database.clickhouse.connection import get_clickhouse_client
                client = await get_clickhouse_client()
                if client:
                    checks.append(("ClickHouse", "✅", "连接正常"))
            except Exception as e:
                checks.append(("ClickHouse", "❌", str(e)[:50]))

            # 检查任务队列
            jobs = self.scheduler.get_jobs()
            checks.append(("定时任务", "✅", f"{len(jobs)} 个任务运行中"))

            # 记录检查结果
            for service, status, info in checks:
                logger.info(f"  {service}: {status} {info}")

            # 如果有失败的服务，发送告警
            failed_services = [s for s, status, _ in checks if status == "❌"]
            if failed_services:
                logger.warning(f"⚠️ 服务异常: {', '.join(failed_services)}")

            logger.info("✅ 健康检查完成")

        except Exception as e:
            logger.error(f"执行健康检查任务失败: {e}")

    async def _generate_daily_report(self):
        """生成每日统计报告"""
        logger.info("开始生成每日统计报告...")

        try:
            from services.birthday_reminder import BirthdayReminderService
            service = BirthdayReminderService()

            for institution_code in settings.APP.INSTITUTIONS:
                try:
                    report = await service.get_upcoming_birthdays_report(
                        institution_code,
                        days=settings.APP.BIRTHDAY_REMINDER_DAYS_AHEAD
                    )

                    logger.info(f"[{institution_code}] 即将生日客户: {report['total_customers']} 人 (VIP: {report['vip_customers']})")

                except Exception as e:
                    logger.error(f"生成 {institution_code} 报告失败: {e}")

            logger.info("✅ 每日统计报告生成完成")

        except Exception as e:
            logger.error(f"生成每日统计报告失败: {e}")

    def shutdown(self):
        """关闭调度器"""
        if not self.is_running:
            return

        logger.info("正在关闭定时任务调度器...")

        try:
            self.scheduler.shutdown()
            self.is_running = False

            # 关闭数据服务连接
            if self._data_service:
                asyncio.create_task(self._data_service.close_connections())

            logger.info("✅ 定时任务调度器已关闭")

        except Exception as e:
            logger.error(f"关闭定时任务调度器失败: {e}")

    def get_job_info(self) -> Dict[str, Any]:
        """获取任务信息"""
        jobs = self.scheduler.get_jobs()

        job_info = []
        for job in jobs:
            job_info.append({
                'id': job.id,
                'name': job.name,
                'next_run_time': job.next_run_time.isoformat() if job.next_run_time else None,
                'trigger': str(job.trigger)
            })

        return {
            'total_jobs': len(jobs),
            'jobs': job_info,
            'is_running': self.is_running
        }

    def run_job_now(self, job_id: str) -> bool:
        """立即运行一次任务"""
        try:
            job = self.scheduler.get_job(job_id)
            if job:
                job.modify(next_run_time=datetime.now())
                logger.info(f"已触发任务: {job_id}")
                return True
            else:
                logger.error(f"任务不存在: {job_id}")
                return False
        except Exception as e:
            logger.error(f"触发任务失败: {job_id}, 错误: {e}")
            return False

    def list_jobs(self):
        """打印任务列表"""
        jobs = self.scheduler.get_jobs()
        print("\n" + "=" * 60)
        print("  定时任务列表")
        print("=" * 60)
        for job in jobs:
            next_run = job.next_run_time.strftime('%Y-%m-%d %H:%M:%S') if job.next_run_time else 'N/A'
            print(f"  [{job.id}] {job.name}")
            print(f"      下次运行: {next_run}")
            print(f"      触发器: {job.trigger}")
        print("=" * 60)


# 全局调度器实例
scheduler_manager: Optional[SchedulerManager] = None


def get_scheduler_manager() -> SchedulerManager:
    """获取调度器管理实例"""
    global scheduler_manager
    if scheduler_manager is None:
        scheduler_manager = SchedulerManager()
    return scheduler_manager


async def run_scheduler():
    """运行调度器"""
    manager = get_scheduler_manager()
    manager.start()
    manager.list_jobs()

    logger.info("调度器运行中，按 Ctrl+C 停止...")

    # 保持运行
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\n")
        manager.shutdown()


if __name__ == "__main__":
    # 运行调度器
    asyncio.run(run_scheduler())