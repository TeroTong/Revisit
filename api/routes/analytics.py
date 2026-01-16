"""
数据分析API路由
"""
from typing import Optional
from datetime import date, timedelta
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from pydantic import BaseModel

from services.analytics_service import AnalyticsService

router = APIRouter(prefix="/analytics", tags=["数据分析"])


class ReminderStatsResponse(BaseModel):
    """回访统计响应"""
    institution_code: str
    period: dict
    statistics: list
    total_count: int
    success_rate: float


@router.get("/{institution_code}/reminder-stats")
async def get_reminder_statistics(
        institution_code: str = Path(..., description="机构代码"),
        start_date: date = Query(None, description="开始日期"),
        end_date: date = Query(None, description="结束日期")
):
    """获取回访统计"""

    try:
        # 如果没有提供日期，使用最近30天
        if not start_date:
            start_date = date.today() - timedelta(days=30)
        if not end_date:
            end_date = date.today()

        stats = await AnalyticsService.get_reminder_statistics(
            institution_code, start_date, end_date
        )

        if not stats:
            return {
                "institution_code": institution_code,
                "period": {"start": start_date.isoformat(), "end": end_date.isoformat()},
                "statistics": [],
                "total_count": 0,
                "success_rate": 0.0
            }

        # 计算总数和成功率
        total_count = sum(s['total_count'] for s in stats['statistics'])
        success_count = sum(s['success_count'] for s in stats['statistics'])
        success_rate = (success_count / total_count * 100) if total_count > 0 else 0.0

        return ReminderStatsResponse(
            institution_code=stats['institution_code'],
            period=stats['period'],
            statistics=stats['statistics'],
            total_count=total_count,
            success_rate=success_rate
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取回访统计失败: {str(e)}")


@router.get("/{institution_code}/customer-behavior")
async def get_customer_behavior_analysis(
        institution_code: str = Path(..., description="机构代码"),
        customer_id: Optional[int] = Query(None, description="客户ID"),
        days: int = Query(30, ge=1, le=365, description="分析天数")
):
    """获取客户行为分析"""

    try:
        analysis = await AnalyticsService.get_customer_behavior_analysis(
            institution_code, customer_id, days
        )

        return analysis

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取客户行为分析失败: {str(e)}")


@router.get("/{institution_code}/birthday-effectiveness")
async def get_birthday_reminder_effectiveness(
        institution_code: str = Path(..., description="机构代码"),
        year: Optional[int] = Query(None, description="年份")
):
    """获取生日回访效果分析"""

    try:
        if year is None:
            year = date.today().year

        effectiveness = await AnalyticsService.get_birthday_reminder_effectiveness(
            institution_code, year
        )

        return effectiveness

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取生日回访效果分析失败: {str(e)}")


@router.get("/{institution_code}/daily-report")
async def get_daily_report(
        institution_code: str = Path(..., description="机构代码"),
        report_date: Optional[date] = Query(None, description="报告日期")
):
    """获取日报"""

    try:
        report = await AnalyticsService.generate_daily_report(institution_code, report_date)

        return report

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取日报失败: {str(e)}")


@router.get("/{institution_code}/health")
async def get_institution_health(
        institution_code: str = Path(..., description="机构代码")
):
    """获取机构健康状态"""

    try:
        suffix = institution_code.lower().replace("-", "_")

        from database.postgres.connection import get_connection

        async with get_connection() as conn:
            # 检查表是否存在
            tables_to_check = [
                f"institution_customer_{suffix}",
                f"pending_birthday_reminder_{suffix}",
                f"completed_reminder_{suffix}"
            ]

            table_status = {}

            for table in tables_to_check:
                try:
                    check_query = f"SELECT COUNT(*) FROM {table} LIMIT 1"
                    count = await conn.fetchval(check_query)
                    table_status[table] = {
                        "exists": True,
                        "has_data": count > 0 if count is not None else False
                    }
                except Exception as e:
                    table_status[table] = {
                        "exists": False,
                        "error": str(e)
                    }

            # 获取基本统计
            if table_status[f"institution_customer_{suffix}"]["exists"]:
                customer_query = f'''
                    SELECT 
                        COUNT(*) as total_customers,
                        COUNT(CASE WHEN is_vip = TRUE THEN 1 END) as vip_customers,
                        COUNT(CASE WHEN last_visit_date >= CURRENT_DATE - INTERVAL '30 days' THEN 1 END) as active_customers
                    FROM institution_customer_{suffix}
                '''

                customer_stats = await conn.fetchrow(customer_query)
            else:
                customer_stats = None

            return {
                "institution_code": institution_code,
                "timestamp": date.today().isoformat(),
                "table_status": table_status,
                "customer_stats": {
                    "total": customer_stats["total_customers"] if customer_stats else 0,
                    "vip": customer_stats["vip_customers"] if customer_stats else 0,
                    "active": customer_stats["active_customers"] if customer_stats else 0
                } if customer_stats else None
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取机构健康状态失败: {str(e)}")