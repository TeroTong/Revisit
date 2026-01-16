"""
基础回访服务
"""
import asyncio
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from datetime import datetime

from config.settings import settings
from core.models.customer import Customer
from core.models.reminder import Reminder, ReminderType, ReminderChannel
from core.services.data_service import DataService


class BaseReminderService(ABC):
    """基础回访服务"""

    def __init__(self):
        self.data_service = DataService()

    @abstractmethod
    async def get_reminder_type(self) -> ReminderType:
        """获取回访类型"""
        pass

    @abstractmethod
    async def get_reminder_customers(self, institution_code: str) -> List[Customer]:
        """获取需要回访的客户"""
        pass

    @abstractmethod
    async def generate_reminder_content(self, customer: Customer, history: Dict[str, Any]) -> str:
        """生成回访内容"""
        pass

    async def create_reminders(self, institution_code: str) -> List[Reminder]:
        """创建回访任务"""
        customers = await self.get_reminder_customers(institution_code)
        reminders = []

        for customer in customers:
            reminder_date = await self.calculate_reminder_date(customer)
            reminder = await self.data_service.create_birthday_reminder(
                customer, reminder_date, await self.get_priority(customer)
            )

            if reminder:
                reminders.append(reminder)

        return reminders

    async def calculate_reminder_date(self, customer: Customer) -> datetime.date:
        """计算回访日期"""
        # 默认提前7天
        days_ahead = settings.APP.BIRTHDAY_REMINDER_DAYS_AHEAD
        birthday_this_year = customer.get_birthday_this_year()
        return birthday_this_year - datetime.timedelta(days=days_ahead)

    async def get_priority(self, customer: Customer) -> int:
        """获取优先级"""
        # 默认逻辑：VIP客户优先级高
        if customer.is_vip:
            return 1
        elif customer.level in ["金卡会员", "铂金会员", "钻石会员"]:
            return 2
        else:
            return 3

    async def process_reminders(self, institution_code: str, batch_size: int = None):
        """处理回访任务"""
        if batch_size is None:
            batch_size = settings.APP.REMINDER_BATCH_SIZE

        # 获取待处理的回访任务
        reminders = await self.data_service.get_pending_reminders(
            institution_code, await self.get_reminder_type()
        )

        # 分批处理
        for i in range(0, len(reminders), batch_size):
            batch = reminders[i:i + batch_size]
            tasks = [self.process_single_reminder(reminder) for reminder in batch]
            await asyncio.gather(*tasks)

    async def process_single_reminder(self, reminder: Reminder):
        """处理单个回访任务"""
        try:
            # 标记为处理中
            reminder.mark_processing()
            await self.data_service.update_reminder_status(reminder, reminder.status)

            # 获取客户信息
            customer = await self.data_service.get_customer_by_id(
                reminder.customer_id, reminder.institution_code
            )

            if not customer:
                raise Exception(f"客户不存在: {reminder.customer_id}")

            # 获取客户历史记录
            history = await self.data_service.get_customer_history(
                reminder.customer_id, reminder.institution_code
            )

            # 生成回访内容
            content = await self.generate_reminder_content(customer, history)

            # 发送通知
            success = await self.send_notification(customer, content, reminder.channel)

            if success:
                # 标记为完成
                reminder.mark_completed(content=content)
                await self.data_service.update_reminder_status(
                    reminder, reminder.status, content
                )
            else:
                # 标记为失败
                reminder.mark_failed("发送通知失败")
                await self.data_service.update_reminder_status(
                    reminder, reminder.status, error_message="发送通知失败"
                )

        except Exception as e:
            # 标记为失败
            reminder.mark_failed(str(e))
            await self.data_service.update_reminder_status(
                reminder, reminder.status, error_message=str(e)
            )

    async def send_notification(self, customer: Customer, content: str, channel: ReminderChannel) -> bool:
        """发送通知"""
        # 这里应该调用具体的通知服务
        # 暂时返回成功
        print(f"发送通知给 {customer.name} ({customer.phone}) 通过 {channel}: {content[:50]}...")
        return True