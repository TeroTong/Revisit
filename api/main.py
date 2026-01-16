"""
FastAPI主应用
"""
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import logging

from config.settings import settings
from api.routes import customers, reminders, analytics
from database.postgres.connection import create_pool, close_pool

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时
    logger.info("正在启动API服务...")

    # 初始化数据库连接池
    await create_pool()


    yield

    # 关闭时
    logger.info("正在关闭API服务...")

    # 关闭数据库连接池
    await close_pool()


# 创建FastAPI应用
app = FastAPI(
    title="医美客户回访系统 API",
    version=settings.APP.APP_VERSION,
    description="医美客户回访系统的RESTful API接口",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境中应该限制来源
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(customers.router, prefix="/api/v1", tags=["客户管理"])
app.include_router(reminders.router, prefix="/api/v1", tags=["回访管理"])
app.include_router(analytics.router, prefix="/api/v1", tags=["数据分析"])


# 全局异常处理
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error(f"全局异常: {exc}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"message": "服务器内部错误", "detail": str(exc)}
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"message": exc.detail}
    )


# 健康检查
@app.get("/health", tags=["系统"])
async def health_check():
    """健康检查"""
    return {
        "status": "healthy",
        "service": "medical-beauty-reminder-api",
        "version": settings.APP.APP_VERSION
    }


# 根路径
@app.get("/", tags=["系统"])
async def root():
    """根路径"""
    return {
        "name": "医美客户回访系统 API",
        "version": settings.APP.APP_VERSION,
        "docs": "/docs",
        "health": "/health"
    }