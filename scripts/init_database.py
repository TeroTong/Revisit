# init_database.py
"""
数据库初始化脚本

支持初始化以下数据库：
- PostgreSQL: 主数据库（OLTP）
- NebulaGraph: 图数据库（关系分析）
- ClickHouse: 分析数据库（OLAP）
- Qdrant: 向量数据库（语义搜索）

使用方法：
    python scripts/init_database.py              # 初始化所有数据库
    python scripts/init_database.py --check-only # 仅检查连接
    python scripts/init_database.py --skip-nebula --skip-qdrant  # 跳过指定数据库
    python scripts/init_database.py --force      # 强制重新初始化（危险！）
"""

import os

# 禁用代理设置（解决远程数据库连接问题）
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    os.environ.pop(key, None)

import asyncio
import sys
import logging
import argparse
from pathlib import Path
from typing import List
from dataclasses import dataclass
from enum import Enum

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root.absolute()))

from config.settings import settings
from database.postgres.connection import create_pool, close_pool
from database.postgres.migrations import DatabaseMigrator
from database.nebula.schema import init_nebula_schema
from database.clickhouse.schema import init_clickhouse_schema

# 配置日志
logging.basicConfig(
    level=settings.APP.LOG_LEVEL,
    format=settings.APP.LOG_FORMAT,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(settings.APP.LOG_DIR / "init_databases.log", encoding='utf-8')
    ]
)

logger = logging.getLogger(__name__)


class DatabaseStatus(Enum):
    """数据库状态枚举"""
    SUCCESS = "✅"
    WARNING = "⚠️"
    FAILED = "❌"


@dataclass
class DatabaseCheckResult:
    """数据库检查结果"""
    name: str
    status: DatabaseStatus
    message: str


class DatabaseInitializer:
    """数据库初始化器"""

    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.results: List[DatabaseCheckResult] = []

    async def initialize_all(self) -> bool:
        """初始化所有数据库"""
        logger.info("=" * 60)
        logger.info("开始初始化数据库")
        logger.info(f"环境: {settings.APP.ENVIRONMENT}")
        logger.info("=" * 60)

        # 等待 Docker 服务启动
        if settings.APP.ENVIRONMENT != "test" and self.args.wait_docker > 0:
            logger.info(f"等待 {self.args.wait_docker} 秒确保Docker服务就绪...")
            await asyncio.sleep(self.args.wait_docker)

        success = True

        # 按依赖顺序初始化：PostgreSQL -> NebulaGraph -> ClickHouse -> Qdrant
        if not self.args.skip_postgres:
            if self.args.force:
                logger.warning("⚠️ 强制重新初始化，将删除现有数据...")
                await self._force_reinitialize_postgresql()

            if not await self._initialize_postgresql():
                logger.error("PostgreSQL初始化失败，退出")
                return False
        else:
            logger.info("跳过PostgreSQL初始化")

        if not self.args.skip_nebula:
            if not await self._initialize_nebula():
                logger.warning("NebulaGraph初始化失败，系统将继续运行")
                success = False
        else:
            logger.info("跳过NebulaGraph初始化")

        if not self.args.skip_clickhouse:
            if not await self._initialize_clickhouse():
                logger.warning("ClickHouse初始化失败，系统将继续运行")
                success = False
        else:
            logger.info("跳过ClickHouse初始化")

        if not self.args.skip_qdrant:
            if not await self._initialize_qdrant():
                logger.warning("Qdrant初始化失败，系统将继续运行")
                success = False
        else:
            logger.info("跳过Qdrant初始化")

        # 最终检查
        logger.info("执行最终连接检查...")
        all_healthy = await self.check_all_connections()

        self._print_final_report(all_healthy)

        return all_healthy

    async def _initialize_postgresql(self) -> bool:
        """初始化PostgreSQL"""
        logger.info("正在初始化PostgreSQL...")

        try:
            import asyncpg

            # 清理现有表
            await self._clean_postgresql_tables()

            # 确保数据库存在
            if not await self._ensure_postgresql_database():
                return False

            # 运行迁移
            logger.info("正在创建数据库连接池...")
            pool = await create_pool()

            async with pool.acquire() as conn:
                logger.info("正在运行数据库迁移...")
                # 只在开发/测试环境且明确指定时才插入示例数据
                with_sample_data = self.args.with_sample_data and settings.APP.ENVIRONMENT != "production"
                migrator = DatabaseMigrator(conn, with_sample_data=with_sample_data)
                await migrator.migrate_all()

            logger.info("✅ PostgreSQL初始化完成")
            return True

        except Exception as e:
            logger.error(f"PostgreSQL初始化失败: {e}", exc_info=True)
            return False

    async def _clean_postgresql_tables(self):
        """清理PostgreSQL现有表"""
        try:
            import asyncpg

            logger.info("正在清理现有表...")
            conn = await asyncpg.connect(
                host=settings.DATABASE.POSTGRES_HOST,
                port=settings.DATABASE.POSTGRES_PORT,
                user=settings.DATABASE.POSTGRES_USER,
                password=settings.DATABASE.POSTGRES_PASSWORD,
                database=settings.DATABASE.POSTGRES_DB
            )

            tables = await conn.fetch("""
                SELECT tablename FROM pg_tables 
                WHERE schemaname = 'public'
                ORDER BY tablename
            """)

            for table in tables:
                try:
                    await conn.execute(f'DROP TABLE IF EXISTS "{table["tablename"]}" CASCADE')
                except Exception as e:
                    logger.warning(f"删除表 {table['tablename']} 失败: {e}")

            await conn.close()
            logger.info("✅ 现有表清理完成")

        except Exception as e:
            logger.warning(f"清理现有表时出错，继续执行: {e}")

    async def _ensure_postgresql_database(self) -> bool:
        """确保PostgreSQL数据库存在"""
        import asyncpg

        try:
            # 尝试直接连接
            logger.info(f"尝试连接到数据库 '{settings.DATABASE.POSTGRES_DB}'...")
            conn = await asyncpg.connect(
                host=settings.DATABASE.POSTGRES_HOST,
                port=settings.DATABASE.POSTGRES_PORT,
                user=settings.DATABASE.POSTGRES_USER,
                password=settings.DATABASE.POSTGRES_PASSWORD,
                database=settings.DATABASE.POSTGRES_DB
            )
            await conn.close()
            logger.info(f"✅ 数据库 '{settings.DATABASE.POSTGRES_DB}' 已存在")
            return True

        except asyncpg.InvalidCatalogNameError:
            # 数据库不存在，创建它
            return await self._create_postgresql_database()

        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            return False

    async def _create_postgresql_database(self) -> bool:
        """创建PostgreSQL数据库"""
        import asyncpg

        logger.info(f"数据库 '{settings.DATABASE.POSTGRES_DB}' 不存在，开始创建...")

        try:
            admin_conn = await asyncpg.connect(
                host=settings.DATABASE.POSTGRES_HOST,
                port=settings.DATABASE.POSTGRES_PORT,
                user=settings.DATABASE.POSTGRES_USER,
                password=settings.DATABASE.POSTGRES_PASSWORD,
                database='postgres',
                timeout=10
            )

            await admin_conn.execute(f"""
                CREATE DATABASE "{settings.DATABASE.POSTGRES_DB}"
                WITH ENCODING = 'UTF8'
                LC_COLLATE = 'en_US.UTF-8'
                LC_CTYPE = 'en_US.UTF-8'
                TEMPLATE = template0;
            """)

            await admin_conn.close()
            logger.info(f"✅ 数据库 '{settings.DATABASE.POSTGRES_DB}' 创建成功")
            return True

        except asyncpg.InsufficientPrivilegeError:
            logger.error(f"权限不足，无法创建数据库")
            return False
        except Exception as e:
            logger.error(f"创建数据库失败: {e}")
            return False

    async def _initialize_nebula(self) -> bool:
        """初始化NebulaGraph"""
        logger.info("正在初始化NebulaGraph...")

        try:
            from database.nebula.connection import NebulaConnection

            success = NebulaConnection.initialize_with_config(
                space_name=settings.DATABASE.NEBULA_SPACE,
                auto_add_hosts=getattr(settings.DATABASE, 'NEBULA_AUTO_ADD_HOSTS', False)
            )

            if success:
                logger.info("初始化NebulaGraph Schema...")
                await init_nebula_schema()
                logger.info("✅ NebulaGraph初始化完成")
                return True
            else:
                logger.error("❌ NebulaGraph集群初始化失败")
                return False

        except Exception as e:
            logger.error(f"NebulaGraph初始化失败: {e}", exc_info=True)
            return False

    async def _initialize_clickhouse(self) -> bool:
        """初始化ClickHouse"""
        logger.info("正在初始化ClickHouse...")

        try:
            success = await init_clickhouse_schema()
            if success:
                logger.info("✅ ClickHouse初始化完成")
            return success

        except Exception as e:
            logger.error(f"ClickHouse初始化失败: {e}")
            return False

    async def _initialize_qdrant(self) -> bool:
        """初始化Qdrant向量数据库"""
        logger.info("正在初始化Qdrant向量数据库...")

        try:
            from database.qdrant.schema import init_qdrant_schema

            success = init_qdrant_schema(cleanup_extra=True)

            if success:
                logger.info("✅ Qdrant向量数据库初始化完成")
                return True

            logger.warning("Qdrant初始化失败，尝试兼容版本...")
            return self._try_qdrant_compatible()

        except Exception as e:
            logger.error(f"Qdrant初始化失败: {e}", exc_info=True)
            return False

    def _try_qdrant_compatible(self) -> bool:
        """尝试Qdrant兼容版本"""
        try:
            from database.qdrant.schema_compatible import init_qdrant_schema_compatible
            success = init_qdrant_schema_compatible()
            if success:
                logger.info("✅ Qdrant兼容版本初始化完成")
            return success
        except ImportError:
            logger.warning("Qdrant兼容版本不可用")
            return False

    async def _force_reinitialize_postgresql(self):
        """强制重新初始化PostgreSQL"""
        try:
            import asyncpg

            admin_conn = await asyncpg.connect(
                host=settings.DATABASE.POSTGRES_HOST,
                port=settings.DATABASE.POSTGRES_PORT,
                user=settings.DATABASE.POSTGRES_USER,
                password=settings.DATABASE.POSTGRES_PASSWORD,
                database='postgres'
            )

            await admin_conn.execute(f'DROP DATABASE IF EXISTS "{settings.DATABASE.POSTGRES_DB}"')
            await admin_conn.execute(f'''
                CREATE DATABASE "{settings.DATABASE.POSTGRES_DB}"
                WITH ENCODING = 'UTF8'
                LC_COLLATE = 'en_US.UTF-8'
                LC_CTYPE = 'en_US.UTF-8'
                TEMPLATE = template0;
            ''')

            await admin_conn.close()
            logger.info("✅ 已强制重新创建数据库")

        except Exception as e:
            logger.error(f"强制重新初始化失败: {e}")
            raise

    async def check_all_connections(self) -> bool:
        """检查所有数据库连接"""
        logger.info("正在检查数据库连接...")
        self.results.clear()

        await self._check_postgresql()
        await self._check_nebula()
        await self._check_clickhouse()
        await self._check_qdrant()

        # 打印结果
        logger.info("数据库连接检查结果:")
        logger.info("-" * 50)
        for result in self.results:
            logger.info(f"{result.name:15} {result.status.value:3} {result.message}")
        logger.info("-" * 50)

        return all(r.status != DatabaseStatus.FAILED for r in self.results)

    async def _check_postgresql(self):
        """检查PostgreSQL连接"""
        try:
            pool = await create_pool()
            async with pool.acquire() as conn:
                version = await conn.fetchval("SELECT version()")
                table_count = await conn.fetchval('''
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_schema = 'public'
                ''')
                self.results.append(DatabaseCheckResult(
                    "PostgreSQL", DatabaseStatus.SUCCESS,
                    f"{version.split()[0]}, {table_count} 张表"
                ))
            await close_pool()
        except Exception as e:
            self.results.append(DatabaseCheckResult("PostgreSQL", DatabaseStatus.FAILED, str(e)))

    async def _check_nebula(self):
        """检查NebulaGraph连接"""
        try:
            from database.nebula.connection import get_nebula_session, execute_ngql
            result = execute_ngql("SHOW HOSTS")
            if result and result.is_succeeded():
                host_count = result.row_size()
                self.results.append(DatabaseCheckResult(
                    "NebulaGraph", DatabaseStatus.SUCCESS,
                    f"连接正常，有 {host_count} 个主机"
                ))
            else:
                error_msg = result.error_msg() if result else "未知错误"
                self.results.append(DatabaseCheckResult("NebulaGraph", DatabaseStatus.FAILED, error_msg))
        except Exception as e:
            self.results.append(DatabaseCheckResult("NebulaGraph", DatabaseStatus.FAILED, str(e)))

    async def _check_clickhouse(self):
        """检查ClickHouse连接"""
        try:
            from database.clickhouse.connection import get_clickhouse_client, close_clickhouse_client
            client = await get_clickhouse_client()
            row = await client.fetchrow("SELECT version()")
            version = row[0] if row else "未知"
            self.results.append(DatabaseCheckResult("ClickHouse", DatabaseStatus.SUCCESS, version))
            await close_clickhouse_client()
        except Exception as e:
            self.results.append(DatabaseCheckResult("ClickHouse", DatabaseStatus.FAILED, str(e)))

    async def _check_qdrant(self):
        """检查Qdrant连接"""
        try:
            from database.qdrant.connection import test_qdrant_connection_simple
            from database.qdrant.schema import get_qdrant_collections_info, get_configured_collections

            if not test_qdrant_connection_simple():
                self.results.append(DatabaseCheckResult("Qdrant", DatabaseStatus.FAILED, "连接测试失败"))
                return

            collections_info = get_qdrant_collections_info()

            if "error" in collections_info:
                self.results.append(DatabaseCheckResult("Qdrant", DatabaseStatus.FAILED, f"错误: {collections_info['error']}"))
                return

            collections = collections_info.get("collections", {})
            expected = get_configured_collections()
            existing_expected = [n for n in expected if n in collections]
            extra = [n for n in collections if n not in expected]

            if len(existing_expected) == len(expected) and not extra:
                self.results.append(DatabaseCheckResult("Qdrant", DatabaseStatus.SUCCESS, f"集合数: {len(collections)}"))
            elif extra:
                self.results.append(DatabaseCheckResult("Qdrant", DatabaseStatus.WARNING, f"多余集合: {extra}"))
            else:
                missing = [n for n in expected if n not in collections]
                self.results.append(DatabaseCheckResult("Qdrant", DatabaseStatus.WARNING, f"缺少: {missing}"))

        except Exception as e:
            self.results.append(DatabaseCheckResult("Qdrant", DatabaseStatus.FAILED, str(e)))

    def _print_final_report(self, all_healthy: bool):
        """打印最终报告"""
        logger.info("=" * 60)
        if all_healthy:
            logger.info("✅ 数据库初始化流程完成 - 所有数据库连接正常")
        else:
            logger.warning("⚠️ 数据库初始化完成，但部分数据库连接有问题")
            logger.warning("某些功能可能无法正常使用")

            # 显示 Qdrant 集合详情
            self._print_qdrant_details()
        logger.info("=" * 60)

    def _print_qdrant_details(self):
        """打印Qdrant集合详情"""
        try:
            from database.qdrant.schema import get_qdrant_collections_info
            collections_info = get_qdrant_collections_info()

            if "collections" in collections_info:
                logger.info("Qdrant集合状态:")
                for name, info in collections_info["collections"].items():
                    status = "✅" if info.get("status") == "healthy" else "❌"
                    vectors = info.get("vectors_count", 0)
                    points = info.get("points_count", 0)
                    logger.info(f"  {name:25} {status} 向量: {vectors}, 点: {points}")
        except Exception:
            pass


def parse_args() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="初始化数据库",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python scripts/init_database.py                      # 初始化所有数据库（不含示例数据）
  python scripts/init_database.py --with-sample-data  # 初始化并插入示例数据（仅开发环境）
  python scripts/init_database.py --check-only        # 仅检查连接
  python scripts/init_database.py --skip-nebula       # 跳过NebulaGraph
  python scripts/init_database.py --force             # 强制重新初始化
        """
    )

    parser.add_argument("--check-only", "-c", action="store_true",
                        help="仅检查数据库连接，不初始化")
    parser.add_argument("--with-sample-data", action="store_true",
                        help="插入示例数据（仅用于开发/测试环境，生产环境会忽略此选项）")
    parser.add_argument("--skip-postgres", action="store_true",
                        help="跳过PostgreSQL初始化")
    parser.add_argument("--skip-nebula", action="store_true",
                        help="跳过NebulaGraph初始化")
    parser.add_argument("--skip-clickhouse", action="store_true",
                        help="跳过ClickHouse初始化")
    parser.add_argument("--skip-qdrant", action="store_true",
                        help="跳过Qdrant初始化")
    parser.add_argument("--force", "-f", action="store_true",
                        help="强制重新初始化（谨慎使用）")
    parser.add_argument("--wait-docker", type=int, default=5,
                        help="等待Docker服务启动的秒数（默认: 5）")

    return parser.parse_args()


async def main():
    """主函数"""
    args = parse_args()

    if args.check_only:
        initializer = DatabaseInitializer(args)
        await initializer.check_all_connections()
        return

    initializer = DatabaseInitializer(args)
    success = await initializer.initialize_all()

    if not success:
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("用户中断初始化")
        sys.exit(0)
    except Exception as e:
        logger.error(f"初始化失败: {e}", exc_info=True)
        sys.exit(1)

