"""
ClickHouse连接管理
"""
from typing import Optional
from aiochclient import ChClient
from aiohttp import ClientSession
import logging

from config.settings import settings

logger = logging.getLogger(__name__)


class ClickHouseConnection:
    """ClickHouse连接管理器"""

    _client: Optional[ChClient] = None
    _session: Optional[ClientSession] = None

    @classmethod
    async def get_client(cls):
        """获取ClickHouse客户端"""
        if cls._client is None:
            try:
                # 创建HTTP会话
                cls._session = ClientSession()

                # 创建ClickHouse客户端
                cls._client = ChClient(
                    cls._session,
                    url=f"http://{settings.DATABASE.CLICKHOUSE_HOST}:{settings.DATABASE.CLICKHOUSE_PORT}",
                    user=settings.DATABASE.CLICKHOUSE_USER,
                    password=settings.DATABASE.CLICKHOUSE_PASSWORD,
                    database=settings.DATABASE.CLICKHOUSE_DB,
                )

                # 测试连接
                await cls._client.is_alive()

                logger.info(f"✅ ClickHouse连接成功: {settings.DATABASE.CLICKHOUSE_HOST}")
            except Exception as e:
                logger.error(f"❌ ClickHouse连接失败: {e}")
                if cls._session:
                    await cls._session.close()
                raise

        return cls._client

    @classmethod
    async def close_client(cls):
        """关闭客户端"""
        if cls._client:
            await cls._client.close()
            cls._client = None

        if cls._session:
            await cls._session.close()
            cls._session = None

        logger.info("✅ ClickHouse连接已关闭")


# 全局客户端实例
client: Optional[ChClient] = None


async def get_clickhouse_client() -> ChClient:
    """获取ClickHouse客户端"""
    global client
    if client is None:
        conn = ClickHouseConnection()
        client = await conn.get_client()
    return client


async def close_clickhouse_client():
    """关闭ClickHouse客户端"""
    global client
    if client:
        await ClickHouseConnection.close_client()
        client = None


async def execute_query(query: str, *args):
    """执行ClickHouse查询"""
    global client
    if client is None:
        client = await get_clickhouse_client()

    try:
        result = await client.execute(query, *args)
        return result
    except Exception as e:
        logger.error(f"执行ClickHouse查询失败: {query}, 错误: {e}")
        raise