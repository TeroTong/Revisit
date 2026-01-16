"""
API路由初始化
"""
from api.routes.customers import router as customers_router
from api.routes.reminders import router as reminders_router
from api.routes.analytics import router as analytics_router

__all__ = [
    'customers_router',
    'reminders_router',
    'analytics_router'
]