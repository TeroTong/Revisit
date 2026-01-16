"""
PostgreSQL数据库连接管理
"""
import asyncpg
from typing import Optional, AsyncContextManager
from contextlib import asynccontextmanager
from config.settings import settings
from utils.logger import safe_print


class PostgreSQLConnection:
    """PostgreSQL连接管理器"""

    _pool: Optional[asyncpg.Pool] = None

    @classmethod
    async def create_pool(cls):
        """创建连接池"""
        if cls._pool is None:
            try:
                cls._pool = await asyncpg.create_pool(
                    host=settings.DATABASE.POSTGRES_HOST,
                    port=settings.DATABASE.POSTGRES_PORT,
                    user=settings.DATABASE.POSTGRES_USER,
                    password=settings.DATABASE.POSTGRES_PASSWORD,
                    database=settings.DATABASE.POSTGRES_DB,
                    min_size=settings.DATABASE.POSTGRES_POOL_MIN_SIZE,
                    max_size=settings.DATABASE.POSTGRES_POOL_MAX_SIZE,
                    command_timeout=60,
                )
                safe_print(f"✅ PostgreSQL连接池创建成功: {settings.DATABASE.POSTGRES_HOST}")
            except Exception as e:
                safe_print(f"❌ PostgreSQL连接池创建失败: {e}")
                raise

        return cls._pool

    @classmethod
    async def close_pool(cls):
        """关闭连接池"""
        if cls._pool:
            await cls._pool.close()
            cls._pool = None
            safe_print("✅ PostgreSQL连接池已关闭")

    @classmethod
    @asynccontextmanager
    async def get_connection(cls) -> AsyncContextManager[asyncpg.Connection]:
        """获取数据库连接"""
        if cls._pool is None:
            await cls.create_pool()

        conn = await cls._pool.acquire()
        try:
            yield conn
        finally:
            await cls._pool.release(conn)

    @classmethod
    async def execute(cls, query: str, *args):
        """执行SQL语句"""
        async with cls.get_connection() as conn:
            return await conn.execute(query, *args)

    @classmethod
    async def fetch(cls, query: str, *args):
        """执行查询并返回所有结果"""
        async with cls.get_connection() as conn:
            return await conn.fetch(query, *args)

    @classmethod
    async def fetchrow(cls, query: str, *args):
        """执行查询并返回单行结果"""
        async with cls.get_connection() as conn:
            return await conn.fetchrow(query, *args)

    @classmethod
    async def fetchval(cls, query: str, *args):
        """执行查询并返回单个值"""
        async with cls.get_connection() as conn:
            return await conn.fetchval(query, *args)


# 全局连接池实例
pool: Optional[asyncpg.Pool] = None


async def create_pool() -> asyncpg.Pool:
    """创建连接池"""
    global pool
    pool = await PostgreSQLConnection.create_pool()
    return pool


async def close_pool():
    """关闭连接池"""
    global pool
    await PostgreSQLConnection.close_pool()
    pool = None


@asynccontextmanager
async def get_connection():
    """获取连接"""
    async with PostgreSQLConnection.get_connection() as conn:
        yield conn


async def release_connection(conn: asyncpg.Connection):
    """释放连接"""
    if pool and conn:
        await pool.release(conn)