"""
分析服务
"""
from datetime import date, timedelta
from typing import Dict, Any
import logging

from database.clickhouse.connection import execute_query
from database.postgres.connection import get_connection

logger = logging.getLogger(__name__)


class AnalyticsService:
    """分析服务"""

    @staticmethod
    async def get_reminder_statistics(
            institution_code: str,
            start_date: date,
            end_date: date
    ) -> Dict[str, Any]:
        """获取回访统计"""

        try:
            query = '''
                SELECT 
                    reminder_type,
                    channel,
                    COUNT(*) as total_count,
                    SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as success_count,
                    AVG(CASE WHEN success = 1 THEN 1.0 ELSE 0.0 END) * 100 as success_rate
                FROM customer_behavior
                WHERE date >= ? 
                AND date <= ?
                AND institution_code = ?
                AND behavior_type = 'reminder'
                GROUP BY reminder_type, channel
                ORDER BY reminder_type, channel
            '''

            results = await execute_query(query, start_date, end_date, institution_code)

            statistics = []
            for row in results:
                statistics.append({
                    'reminder_type': row[0],
                    'channel': row[1],
                    'total_count': row[2],
                    'success_count': row[3],
                    'success_rate': float(row[4]) if row[4] else 0.0
                })

            return {
                'institution_code': institution_code,
                'period': {'start': start_date, 'end': end_date},
                'statistics': statistics
            }

        except Exception as e:
            logger.error(f"获取回访统计失败: {e}")
            return {}

    @staticmethod
    async def get_customer_behavior_analysis(
            institution_code: str,
            customer_id: int = None,
            days: int = 30
    ) -> Dict[str, Any]:
        """获取客户行为分析"""

        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        try:
            where_clause = "date >= ? AND date <= ? AND institution_code = ?"
            params = [start_date, end_date, institution_code]

            if customer_id:
                where_clause += " AND customer_id = ?"
                params.append(customer_id)

            query = f'''
                SELECT 
                    behavior_type,
                    behavior_subtype,
                    COUNT(*) as count,
                    AVG(amount) as avg_amount,
                    MIN(date) as first_date,
                    MAX(date) as last_date
                FROM customer_behavior
                WHERE {where_clause}
                GROUP BY behavior_type, behavior_subtype
                ORDER BY count DESC
            '''

            results = await execute_query(query, *params)

            analysis = []
            for row in results:
                analysis.append({
                    'behavior_type': row[0],
                    'behavior_subtype': row[1],
                    'count': row[2],
                    'avg_amount': float(row[3]) if row[3] else 0.0,
                    'first_date': row[4],
                    'last_date': row[5]
                })

            return {
                'institution_code': institution_code,
                'customer_id': customer_id,
                'period': {'start': start_date, 'end': end_date},
                'analysis': analysis
            }

        except Exception as e:
            logger.error(f"获取客户行为分析失败: {e}")
            return {}

    @staticmethod
    async def get_birthday_reminder_effectiveness(
            institution_code: str,
            year: int = None
    ) -> Dict[str, Any]:
        """获取生日回访效果分析"""

        if year is None:
            year = date.today().year

        suffix = institution_code.lower().replace("-", "_")

        async with get_connection() as conn:
            # 查询生日回访后的消费情况
            query = f'''
                SELECT 
                    EXTRACT(MONTH FROM cr.reminder_time) as month,
                    COUNT(DISTINCT cr.institution_customer_id) as reminded_customers,
                    COUNT(DISTINCT c.institution_customer_id) as consumed_customers,
                    SUM(CASE WHEN c.institution_customer_id IS NOT NULL THEN 1 ELSE 0 END) as total_consumptions,
                    COALESCE(SUM(c.actual_amount), 0) as total_consumption_amount
                FROM completed_reminder_{suffix} cr
                LEFT JOIN consumption_{suffix} c 
                    ON cr.institution_customer_id = c.institution_customer_id
                    AND c.consumption_date BETWEEN cr.reminder_time AND cr.reminder_time + INTERVAL '30 days'
                WHERE cr.status = 'completed'
                AND EXTRACT(YEAR FROM cr.reminder_time) = $1
                GROUP BY EXTRACT(MONTH FROM cr.reminder_time)
                ORDER BY month
            '''

            rows = await conn.fetch(query, year)

            monthly_data = []
            for row in rows:
                month = int(row['month'])
                reminded = row['reminded_customers']
                consumed = row['consumed_customers']
                consumptions = row['total_consumptions']
                amount = float(row['total_consumption_amount']) if row['total_consumption_amount'] else 0.0

                conversion_rate = (consumed / reminded * 100) if reminded > 0 else 0.0

                monthly_data.append({
                    'month': month,
                    'reminded_customers': reminded,
                    'consumed_customers': consumed,
                    'total_consumptions': consumptions,
                    'total_consumption_amount': amount,
                    'conversion_rate': conversion_rate,
                    'avg_consumption_per_customer': (amount / consumed) if consumed > 0 else 0.0
                })

            return {
                'institution_code': institution_code,
                'year': year,
                'monthly_data': monthly_data
            }

    @staticmethod
    async def generate_daily_report(institution_code: str, report_date: date = None) -> Dict[str, Any]:
        """生成日报"""

        if report_date is None:
            report_date = date.today()

        suffix = institution_code.lower().replace("-", "_")

        async with get_connection() as conn:
            # 获取当日回访统计
            reminder_query = f'''
                SELECT 
                    COUNT(*) as total_reminders,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_reminders,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_reminders
                FROM completed_reminder_{suffix}
                WHERE DATE(reminder_time) = $1
            '''

            reminder_stats = await conn.fetchrow(reminder_query, report_date)

            # 获取当日新增客户
            new_customer_query = f'''
                SELECT COUNT(*) as new_customers
                FROM institution_customer_{suffix}
                WHERE DATE(created_at) = $1
            '''

            new_customer_stats = await conn.fetchrow(new_customer_query, report_date)

            # 获取当日消费统计
            consumption_query = f'''
                SELECT 
                    COUNT(*) as total_consumptions,
                    COUNT(DISTINCT institution_customer_id) as consuming_customers,
                    SUM(actual_amount) as total_amount
                FROM consumption_{suffix}
                WHERE DATE(consumption_date) = $1
            '''

            consumption_stats = await conn.fetchrow(consumption_query, report_date)

            return {
                'report_date': report_date,
                'institution_code': institution_code,
                'reminders': {
                    'total': reminder_stats['total_reminders'] if reminder_stats else 0,
                    'completed': reminder_stats['completed_reminders'] if reminder_stats else 0,
                    'failed': reminder_stats['failed_reminders'] if reminder_stats else 0,
                    'success_rate': (
                        (reminder_stats['completed_reminders'] / reminder_stats['total_reminders'] * 100)
                        if reminder_stats and reminder_stats['total_reminders'] > 0 else 0.0
                    )
                },
                'customers': {
                    'new': new_customer_stats['new_customers'] if new_customer_stats else 0
                },
                'consumption': {
                    'total_count': consumption_stats['total_consumptions'] if consumption_stats else 0,
                    'customer_count': consumption_stats['consuming_customers'] if consumption_stats else 0,
                    'total_amount': float(consumption_stats['total_amount']) if consumption_stats and consumption_stats[
                        'total_amount'] else 0.0,
                    'avg_per_customer': (
                        float(consumption_stats['total_amount']) / consumption_stats['consuming_customers']
                        if consumption_stats and consumption_stats['consuming_customers'] > 0 else 0.0
                    )
                }
            }