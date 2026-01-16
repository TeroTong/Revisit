#!/usr/bin/env python3
"""
医美客户回访系统主入口
"""

import os

# 禁用代理设置（解决远程数据库连接问题）
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    os.environ.pop(key, None)


import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import FileResponse
from loguru import logger
from pathlib import Path

from config.settings import settings
from api.main import app as api_app
from tasks.scheduler import SchedulerManager
from database.postgres.connection import create_pool, close_pool
from database.nebula.connection import get_nebula_session, close_nebula_session

# 配置日志
logging.basicConfig(
    level=settings.APP.LOG_LEVEL,
    format=settings.APP.LOG_FORMAT
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    应用生命周期管理
    """
    # 启动时
    logger.info("正在启动医美客户回访系统...")

    # 初始化数据库连接池
    logger.info("正在初始化数据库连接池...")
    await create_pool()

    # 初始化NebulaGraph连接
    logger.info("正在初始化NebulaGraph连接...")
    try:
        get_nebula_session()  # 同步函数，不需要 await
    except Exception as e:
        logger.warning(f"NebulaGraph连接失败，图数据库功能将不可用: {e}")

    # 启动定时任务调度器
    if settings.APP.SCHEDULER_ENABLED:
        logger.info("正在启动定时任务调度器...")
        scheduler = SchedulerManager()
        scheduler.start()

    yield

    # 关闭时
    logger.info("正在关闭医美客户回访系统...")

    # 关闭数据库连接池
    await close_pool()

    # 关闭NebulaGraph连接
    try:
        close_nebula_session()  # 同步函数，不需要 await
    except Exception as e:
        logger.warning(f"关闭NebulaGraph连接时出错: {e}")

    # 关闭定时任务调度器
    if settings.APP.SCHEDULER_ENABLED:
        scheduler.shutdown()

    logger.info("系统已关闭")


# 创建FastAPI应用
app = FastAPI(
    title="医美客户回访系统",
    version="1.0.0",
    description="基于AI的智能医美客户回访系统",
    lifespan=lifespan
)

# 挂载API应用
app.mount("/api", api_app)


@app.get("/")
async def root():
    """根路径 - 返回前端页面"""
    index_file = Path(__file__).parent / "static" / "index.html"
    if index_file.exists():
        return FileResponse(index_file, media_type="text/html")
    return {
        "name": "医美客户回访系统",
        "version": "1.0.0",
        "status": "running",
        "docs": "/api/docs",
        "health": "/health"
    }


@app.get("/health")
async def health_check():
    """健康检查"""
    try:
        # 检查数据库连接
        from database.postgres.connection import pool
        if pool:
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")

        # 检查NebulaGraph连接
        from database.nebula.connection import NebulaConnection
        nebula_session = NebulaConnection._session
        if nebula_session:
            result = nebula_session.execute("SHOW HOSTS")
            if not result.is_succeeded():
                raise Exception("NebulaGraph连接失败")

        return {
            "status": "healthy",
            "database": "connected",
            "nebula": "connected" if nebula_session else "not initialized",
            "timestamp": asyncio.get_event_loop().time()
        }
    except Exception as e:
        logger.error(f"健康检查失败: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": asyncio.get_event_loop().time()
        }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=settings.APP.HOST,
        port=settings.APP.PORT,
        reload=settings.APP.DEBUG,
        log_level=settings.APP.LOG_LEVEL.lower()
    )