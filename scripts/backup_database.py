#!/usr/bin/env python3
"""
数据库备份脚本
"""
import os

# 禁用代理设置（解决远程数据库连接问题）
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    os.environ.pop(key, None)

import asyncio
import sys
import logging
from datetime import datetime
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import settings
from database.postgres.connection import get_connection
from utils.file_utils import FileUtils

# 配置日志
logging.basicConfig(
    level=settings.APP.LOG_LEVEL,
    format=settings.APP.LOG_FORMAT,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(settings.APP.LOG_DIR / "backup.log", encoding='utf-8')
    ]
)

logger = logging.getLogger(__name__)


async def backup_postgresql():
    """备份PostgreSQL数据库"""
    logger.info("开始备份PostgreSQL数据库...")

    try:
        # 创建备份目录
        backup_dir = Path(settings.APP.DATA_DIR) / "backups" / "postgresql"
        backup_dir.mkdir(parents=True, exist_ok=True)

        # 生成备份文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = backup_dir / f"backup_{timestamp}.sql"

        # 使用pg_dump进行备份
        import subprocess

        # 构建pg_dump命令
        cmd = [
            "pg_dump",
            "-h", settings.DATABASE.POSTGRES_HOST,
            "-p", str(settings.DATABASE.POSTGRES_PORT),
            "-U", settings.DATABASE.POSTGRES_USER,
            "-d", settings.DATABASE.POSTGRES_DB,
            "-f", str(backup_file)
        ]

        # 设置环境变量
        env = {
            **dict(os.environ),
            "PGPASSWORD": settings.DATABASE.POSTGRES_PASSWORD
        }

        # 执行备份
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            # 压缩备份文件
            import gzip
            with open(backup_file, 'rb') as f_in:
                with gzip.open(f"{backup_file}.gz", 'wb') as f_out:
                    f_out.write(f_in.read())

            # 删除原始文件
            backup_file.unlink()

            backup_size = Path(f"{backup_file}.gz").stat().st_size / 1024 / 1024  # MB
            logger.info(f"✅ PostgreSQL数据库备份完成: {backup_file}.gz ({backup_size:.2f} MB)")

            # 清理旧备份（保留最近7天）
            await cleanup_old_backups(backup_dir, days=7)

            return True
        else:
            logger.error(f"PostgreSQL数据库备份失败: {result.stderr}")
            return False

    except Exception as e:
        logger.error(f"PostgreSQL数据库备份失败: {e}")
        return False


async def backup_config():
    """备份配置文件"""
    logger.info("开始备份配置文件...")

    try:
        # 创建备份目录
        backup_dir = Path(settings.APP.DATA_DIR) / "backups" / "config"
        backup_dir.mkdir(parents=True, exist_ok=True)

        # 生成备份文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = backup_dir / f"config_{timestamp}.json"

        # 备份配置
        config_data = {
            "database": settings.DATABASE.dict(),
            "app": settings.APP.dict(),
            "llm": settings.LLM.dict() if hasattr(settings, 'LLM') else {},
            "notification": settings.NOTIFICATION.dict() if hasattr(settings, 'NOTIFICATION') else {},
            "backup_time": datetime.now().isoformat()
        }

        # 保存配置
        await FileUtils.write_json(backup_file, config_data)

        logger.info(f"✅ 配置文件备份完成: {backup_file}")
        return True

    except Exception as e:
        logger.error(f"配置文件备份失败: {e}")
        return False


async def cleanup_old_backups(backup_dir: Path, days: int = 7):
    """清理旧备份"""
    try:
        cutoff_time = datetime.now().timestamp() - (days * 24 * 60 * 60)

        deleted_count = 0
        for backup_file in backup_dir.glob("*"):
            if backup_file.is_file() and backup_file.stat().st_mtime < cutoff_time:
                backup_file.unlink()
                deleted_count += 1

        if deleted_count > 0:
            logger.info(f"清理了 {deleted_count} 个旧备份文件")

    except Exception as e:
        logger.error(f"清理旧备份失败: {e}")


async def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="数据库备份")
    parser.add_argument("--postgresql", "-p", action="store_true",
                        help="备份PostgreSQL数据库")
    parser.add_argument("--config", "-c", action="store_true",
                        help="备份配置文件")
    parser.add_argument("--all", "-a", action="store_true",
                        help="备份所有数据")

    args = parser.parse_args()

    success_count = 0
    total_count = 0

    if args.all or args.postgresql:
        total_count += 1
        if await backup_postgresql():
            success_count += 1

    if args.all or args.config:
        total_count += 1
        if await backup_config():
            success_count += 1

    if total_count == 0:
        # 默认备份所有
        total_count = 2
        if await backup_postgresql():
            success_count += 1
        if await backup_config():
            success_count += 1

    logger.info(f"备份完成: {success_count}/{total_count} 成功")

    if success_count < total_count:
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("用户中断备份")
        sys.exit(0)
    except Exception as e:
        logger.error(f"备份失败: {e}")
        sys.exit(1)