#!/usr/bin/env python3
"""
简化版启动脚本 - 用于测试 cpolar 连接
跳过 NebulaGraph 连接，只启动基本的 API 服务
"""

import os
import sys
from pathlib import Path

# 设置 UTF-8 编码
os.environ['PYTHONIOENCODING'] = 'utf-8'
if sys.platform == 'win32':
    os.system('chcp 65001 >nul 2>&1')

# 禁用代理
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    os.environ.pop(key, None)

import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import RedirectResponse, FileResponse

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """简化的生命周期管理"""
    logger.info("正在启动简化版 API 服务...")

    # 只初始化 PostgreSQL
    try:
        from database.postgres.connection import create_pool, close_pool
        await create_pool()
        logger.info("PostgreSQL 连接成功")
    except Exception as e:
        logger.warning(f"PostgreSQL 连接失败: {e}")

    yield

    logger.info("正在关闭服务...")
    try:
        from database.postgres.connection import close_pool
        await close_pool()
    except:
        pass


# 创建 FastAPI 应用
app = FastAPI(
    title="医美客户回访系统 API",
    version="1.0.0",
    description="医美客户回访系统",
    lifespan=lifespan
)

# CORS 中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """返回中文界面"""
    index_file = Path(__file__).parent / "static" / "index.html"
    if index_file.exists():
        return FileResponse(index_file)
    return {"name": "医美客户回访系统", "docs": "/api/docs"}


@app.get("/health")
async def health():
    return {"status": "healthy"}


# 挂载静态文件目录（必须在具体路由之后）
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# 挂载 API 路由（必须在最后，因为会捕获所有 /api/* 请求）
from api.main import app as api_app
app.mount("/api", api_app)


if __name__ == "__main__":
    import uvicorn
    print("\n" + "="*50)
    print("  医美客户回访系统")
    print("  本地访问: http://localhost:8000")
    print("  中文界面: http://localhost:8000/")
    print("  API文档: http://localhost:8000/api/docs")
    print("="*50 + "\n")

    uvicorn.run(
        "start_simple:app",
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )

