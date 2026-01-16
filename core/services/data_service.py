"""
数据服务
"""
import json
from typing import List, Dict, Any, Optional
from datetime import date, datetime, timedelta

from database.postgres.connection import get_connection
from core.models.customer import Customer
from core.models.reminder import Reminder, ReminderType, ReminderStatus


class DataService:
    """数据服务"""

    @staticmethod
    async def get_customer_by_id(customer_id: int, institution_code: str) -> Optional[Customer]:
        """根据ID获取客户"""
        suffix = institution_code.lower().replace("-", "_")
        table_name = f"institution_customer_{suffix}"

        async with get_connection() as conn:
            query = f'''
                SELECT ic.*, np.*
                FROM {table_name} ic
                JOIN natural_person np ON ic.natural_person_id = np.id
                WHERE ic.id = $1
            '''

            row = await conn.fetchrow(query, customer_id)
            if row:
                return Customer.from_db_row(row)

        return None

    @staticmethod
    async def get_customer_by_code(customer_code: str, institution_code: str) -> Optional[Customer]:
        """根据客户代码获取客户"""
        suffix = institution_code.lower().replace("-", "_")
        table_name = f"institution_customer_{suffix}"

        async with get_connection() as conn:
            query = f'''
                SELECT ic.*, np.*
                FROM {table_name} ic
                JOIN natural_person np ON ic.natural_person_id = np.id
                WHERE ic.customer_code = $1
            '''

            row = await conn.fetchrow(query, customer_code)
            if row:
                return Customer.from_db_row(row)

        return None

    @staticmethod
    async def get_today_birthday_customers(institution_code: str) -> List[Customer]:
        """获取今天生日的客户"""
        suffix = institution_code.lower().replace("-", "_")
        table_name = f"institution_customer_{suffix}"
        today = date.today()

        async with get_connection() as conn:
            query = f'''
                SELECT ic.*, np.*
                FROM {table_name} ic
                JOIN natural_person np ON ic.natural_person_id = np.id
                WHERE EXTRACT(MONTH FROM np.birthday) = $1
                AND EXTRACT(DAY FROM np.birthday) = $2
                AND ic.is_vip = TRUE
                AND np.is_active = TRUE
            '''

            rows = await conn.fetch(query, today.month, today.day)
            return [Customer.from_db_row(row) for row in rows]

    @staticmethod
    async def get_upcoming_birthday_customers(
            institution_code: str,
            days_ahead: int = 7
    ) -> List[Customer]:
        """获取即将生日的客户"""
        suffix = institution_code.lower().replace("-", "_")
        table_name = f"institution_customer_{suffix}"
        today = date.today()
        target_date = today + timedelta(days=days_ahead)

        async with get_connection() as conn:
            # 获取在指定天数内生日的客户
            query = f'''
                SELECT ic.*, np.*
                FROM {table_name} ic
                JOIN natural_person np ON ic.natural_person_id = np.id
                WHERE (
                    -- 今年生日
                    (EXTRACT(MONTH FROM np.birthday) = $1 AND EXTRACT(DAY FROM np.birthday) = $2)
                    OR
                    -- 明年生日（如果今年已经过了）
                    (EXTRACT(MONTH FROM np.birthday) = $3 AND EXTRACT(DAY FROM np.birthday) = $4)
                )
                AND ic.is_vip = TRUE
                AND np.is_active = TRUE
            '''

            rows = await conn.fetch(
                query,
                target_date.month, target_date.day,
                today.month, today.day
            )
            return [Customer.from_db_row(row) for row in rows]

    @staticmethod
    async def get_customer_history(
            customer_id: int,
            institution_code: str,
            limit: int = 10
    ) -> Dict[str, Any]:
        """获取客户历史记录"""
        suffix = institution_code.lower().replace("-", "_")

        async with get_connection() as conn:
            # 获取消费记录
            consumption_query = f'''
                SELECT 
                    c.consumption_date,
                    p.name as project_name,
                    p.category as project_category,
                    pr.name as product_name,
                    pr.brand as product_brand,
                    c.amount,
                    c.actual_amount
                FROM consumption_{suffix} c
                LEFT JOIN project p ON c.project_id = p.id
                LEFT JOIN product pr ON c.product_id = pr.id
                WHERE c.institution_customer_id = $1
                ORDER BY c.consumption_date DESC
                LIMIT $2
            '''

            consumptions = await conn.fetch(consumption_query, customer_id, limit)

            # 获取咨询记录
            consultation_query = f'''
                (SELECT 
                    consultation_date,
                    content,
                    'online' as type,
                    recommended_projects
                FROM online_consultation_{suffix}
                WHERE institution_customer_id = $1
                ORDER BY consultation_date DESC
                LIMIT $2)

                UNION ALL

                (SELECT 
                    consultation_date,
                    content,
                    'offline' as type,
                    recommended_projects
                FROM offline_consultation_{suffix}
                WHERE institution_customer_id = $1
                ORDER BY consultation_date DESC
                LIMIT $2)

                ORDER BY consultation_date DESC
                LIMIT $3
            '''

            consultations = await conn.fetch(consultation_query, customer_id, limit, limit * 2)

            return {
                'consumptions': [dict(row) for row in consumptions],
                'consultations': [dict(row) for row in consultations]
            }

    @staticmethod
    async def create_birthday_reminder(
            customer: Customer,
            reminder_date: date,
            priority: int = 1
    ) -> Optional[Reminder]:
        """创建生日回访任务"""
        suffix = customer.institution_code.lower().replace("-", "_")
        table_name = f"pending_birthday_reminder_{suffix}"

        async with get_connection() as conn:
            # 检查是否已存在
            check_query = f'''
                SELECT id FROM {table_name}
                WHERE institution_customer_id = $1
                AND reminder_date = $2
            '''

            existing = await conn.fetchval(check_query, customer.id, reminder_date)

            if existing:
                return None

            # 创建回访任务
            insert_query = f'''
                INSERT INTO {table_name} 
                (institution_customer_id, natural_person_id, birthday, 
                 reminder_date, status, priority, channel)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING *
            '''

            row = await conn.fetchrow(
                insert_query,
                customer.id,
                customer.person_id,
                customer.birthday,
                reminder_date,
                ReminderStatus.PENDING.value,
                priority,
                'wechat'
            )

            if row:
                reminder_data = dict(row)
                reminder_data.update({
                    'reminder_type': ReminderType.BIRTHDAY,
                    'institution_code': customer.institution_code,
                    'customer_id': customer.id,
                    'customer_code': customer.customer_code,
                })
                return Reminder.from_db_row(reminder_data)

        return None

    @staticmethod
    async def get_pending_reminders(
            institution_code: str,
            reminder_type: ReminderType = None
    ) -> List[Reminder]:
        """获取待处理的回访任务"""
        suffix = institution_code.lower().replace("-", "_")

        if reminder_type == ReminderType.BIRTHDAY:
            table_name = f"pending_birthday_reminder_{suffix}"
        else:
            # 其他类型的回访任务表
            raise NotImplementedError(f"Reminder type {reminder_type} not implemented")

        async with get_connection() as conn:
            query = f'''
                SELECT pbr.*, ic.customer_code, np.name as customer_name
                FROM {table_name} pbr
                JOIN institution_customer_{suffix} ic ON pbr.institution_customer_id = ic.id
                JOIN natural_person np ON pbr.natural_person_id = np.id
                WHERE pbr.status = $1
                AND pbr.reminder_date <= CURRENT_DATE
                ORDER BY pbr.priority DESC, pbr.created_at
            '''

            rows = await conn.fetch(query, ReminderStatus.PENDING.value)

            reminders = []
            for row in rows:
                reminder_data = dict(row)
                reminder_data.update({
                    'reminder_type': reminder_type or ReminderType.BIRTHDAY,
                    'institution_code': institution_code,
                    'customer_id': reminder_data['institution_customer_id'],
                })
                reminders.append(Reminder.from_db_row(reminder_data))

            return reminders

    @staticmethod
    async def update_reminder_status(
            reminder: Reminder,
            status: ReminderStatus,
            content: str = None,
            response: str = None,
            error_message: str = None
    ) -> bool:
        """更新回访状态"""
        suffix = reminder.institution_code.lower().replace("-", "_")

        if reminder.reminder_type == ReminderType.BIRTHDAY:
            pending_table = f"pending_birthday_reminder_{suffix}"
            completed_table = f"completed_reminder_{suffix}"
        else:
            raise NotImplementedError(f"Reminder type {reminder.reminder_type} not implemented")

        async with get_connection() as conn:
            async with conn.transaction():
                # 更新待回访表状态
                update_query = f'''
                    UPDATE {pending_table}
                    SET status = $1, updated_at = CURRENT_TIMESTAMP
                    WHERE id = $2
                '''

                await conn.execute(update_query, status.value, reminder.id)

                # 如果是完成或失败，记录到已回访表
                if status in [ReminderStatus.COMPLETED, ReminderStatus.FAILED]:
                    insert_query = f'''
                        INSERT INTO {completed_table}
                        (pending_id, institution_customer_id, natural_person_id,
                         reminder_date, reminder_time, channel, content, response,
                         status, error_message, llm_prompt, llm_response,
                         recommended_projects, recommended_products)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                    '''

                    await conn.execute(
                        insert_query,
                        reminder.id,
                        reminder.customer_id,
                        reminder.customer_code.split('_')[
                            -1] if '_' in reminder.customer_code else reminder.customer_code,
                        reminder.reminder_date,
                        datetime.now(),
                        reminder.channel.value,
                        content,
                        response,
                        status.value,
                        error_message,
                        reminder.llm_prompt,
                        reminder.llm_response,
                        json.dumps(reminder.recommended_projects, ensure_ascii=False),
                        json.dumps(reminder.recommended_products, ensure_ascii=False)
                    )

                return True