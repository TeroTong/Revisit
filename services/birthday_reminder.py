"""
生日回访服务

负责：
1. 查询即将生日的客户
2. 获取客户消费/咨询历史
3. 调用 LLM 生成个性化回访话术
4. 发送回访消息
"""
from datetime import date, datetime
from typing import List, Dict
import logging
import json

from config.settings import settings
from services.data_sync import DataSyncService
from services.llm_service import LLMService
from services.notification_service import NotificationService

logger = logging.getLogger(__name__)


class BirthdayReminderService:
    """生日回访服务"""

    def __init__(self):
        self.data_service = DataSyncService()
        self.llm_service = LLMService()
        self.notification_service = NotificationService()

    async def run_daily_reminders(self, institution_code: str):
        """运行每日生日回访任务

        完整流程：
        1. 查询即将生日的客户
        2. 对每个客户生成个性化回访内容
        3. 发送回访消息
        4. 记录回访结果
        """
        logger.info(f"开始处理 {institution_code} 的生日回访任务")

        try:
            # 1. 获取即将生日的客户
            days_ahead = settings.APP.BIRTHDAY_REMINDER_DAYS_AHEAD
            customers = await self.data_service.get_upcoming_birthday_customers(
                institution_code, days_ahead
            )
            logger.info(f"找到 {len(customers)} 个即将生日的客户")

            if not customers:
                logger.info("没有需要回访的客户")
                return

            # 2. 处理每个客户
            success_count = 0
            fail_count = 0

            for customer in customers:
                try:
                    result = await self.process_single_customer(customer, institution_code)
                    if result:
                        success_count += 1
                    else:
                        fail_count += 1
                except Exception as e:
                    fail_count += 1
                    logger.error(f"处理客户 {customer.get('customer_code')} 失败: {e}")

            logger.info(f"生日回访任务完成: 成功 {success_count}, 失败 {fail_count}")

        except Exception as e:
            logger.error(f"运行生日回访任务失败: {e}")
            raise

    async def process_single_customer(self, customer: Dict, institution_code: str) -> bool:
        """处理单个客户的生日回访"""
        customer_code = customer.get('customer_code')
        customer_id = customer.get('institution_customer_id')
        customer_name = customer.get('name', '客户')
        days_until = customer.get('days_until_birthday', 0)

        logger.info(f"处理客户: {customer_code} ({customer_name}), {days_until}天后生日")

        try:
            # 1. 获取客户消费历史
            history = await self.data_service.get_customer_consumption_history(
                customer_id, institution_code, limit=10
            )

            # 2. 生成回访内容
            content = await self.generate_reminder_content(customer, history)

            if not content:
                logger.warning(f"无法为客户 {customer_code} 生成回访内容")
                return False

            # 3. 发送通知
            phone = customer.get('phone')
            success = False
            if phone:
                success = await self.notification_service.send_sms(phone, content)
                if success:
                    logger.info(f"✅ 成功发送生日回访给 {customer_code}")
                else:
                    logger.warning(f"发送短信失败: {customer_code}")

            # 4. 记录回访结果（即使发送失败也记录）
            await self._record_reminder_result(
                customer, institution_code, content, success
            )

            return success

        except Exception as e:
            logger.error(f"处理客户 {customer_code} 时出错: {e}")
            return False

    async def generate_reminder_content(self, customer: Dict, history: List[Dict]) -> str:
        """生成生日回访内容"""
        try:
            # 构建 prompt
            prompt = self._build_birthday_prompt(customer, history)

            # 调用 LLM
            response = await self.llm_service.generate_content(prompt)

            if response:
                return response
            else:
                # LLM 失败时使用默认内容
                return self._generate_default_content(customer)

        except Exception as e:
            logger.error(f"生成回访内容失败: {e}")
            return self._generate_default_content(customer)

    def _build_birthday_prompt(self, customer: Dict, history: List[Dict]) -> str:
        """构建生日回访提示词"""
        # 计算年龄
        birthday = customer.get('birthday')
        age = None
        if birthday:
            if isinstance(birthday, str):
                birthday = datetime.strptime(birthday, '%Y-%m-%d').date()
            today = date.today()
            age = today.year - birthday.year
            if (today.month, today.day) < (birthday.month, birthday.day):
                age -= 1

        # 格式化消费历史
        history_text = self._format_consumption_history(history)

        prompt = f"""
你是一位专业的医美机构客户关系专员。今天是{date.today().strftime('%Y年%m月%d日')}。

客户信息：
- 姓名：{customer.get('name', '客户')}
- 性别：{customer.get('gender', '未知')}
- 生日：{customer.get('birthday')}（{customer.get('days_until_birthday', 0)}天后）
- 年龄：{age}岁
- 会员等级：{customer.get('vip_level', '普通会员')}
- 累计消费：{customer.get('total_consumption', 0)}元
- 消费次数：{customer.get('consumption_count', 0)}次
- 上次到访：{customer.get('last_visit_date', '未知')}

历史消费记录：
{history_text}

请生成一段个性化的生日祝福短信，要求：
1. 亲切、真诚、温暖
2. 适当提及客户的消费偏好（如果有历史记录）
3. 包含生日专属优惠（如生日月8.8折、双倍积分等）
4. 推荐1-2个适合的项目或产品（基于历史偏好）
5. 控制在200字以内，适合短信发送
6. 不要使用括号或特殊格式符号

请直接输出短信内容，不需要任何前缀说明。
"""
        return prompt

    def _format_consumption_history(self, history: List[Dict]) -> str:
        """格式化消费记录"""
        if not history:
            return "无历史消费记录"

        formatted = []
        for h in history[:5]:  # 只取最近5条
            items = []
            if h.get('project_name'):
                items.append(f"项目：{h['project_name']}")
            if h.get('product_name'):
                items.append(f"产品：{h['product_name']}")

            order_date = h.get('order_date')
            if isinstance(order_date, date):
                order_date = order_date.strftime('%Y-%m-%d')

            amount = h.get('actual_amount', 0)
            formatted.append(f"- {order_date}: {', '.join(items)}，金额：{amount}元")

        return '\n'.join(formatted)

    def _generate_default_content(self, customer: Dict) -> str:
        """生成默认回访内容"""
        name = customer.get('name', '尊敬的客户')
        vip_level = customer.get('vip_level', '会员')

        return f"""亲爱的{name}，

提前祝您生日快乐！感谢您一直以来对我们的信任与支持。

在这特别的日子里，我们为您准备了生日专属礼遇：
★ 生日月消费享8.8折优惠
★ 积分双倍奖励
★ 专属生日礼包

期待您的光临，让我们为您带来更美丽的体验！

【{settings.APP.APP_NAME}】"""

    async def batch_generate_reminder_content(self, customers_data: List[Dict]) -> List[Dict]:
        """批量生成生日回访内容

        使用 asyncio 并发为多个客户同时生成个性化话术
        每个客户单独调用 LLM，确保话术个性化
        使用信号量限制并发数，防止 API 限流
        """
        import asyncio

        results = []

        # 并发限制（同时最多 5 个请求）
        semaphore = asyncio.Semaphore(5)

        # 过滤出有效的客户数据
        valid_customers = [c for c in customers_data if 'customer' in c]
        error_customers = [c for c in customers_data if 'error' in c]

        # 先添加错误的客户
        for c in error_customers:
            results.append({
                "customer_code": c.get("customer_code"),
                "customer_name": "",
                "content": "",
                "success": False,
                "error": c.get("error", "未知错误")
            })

        if not valid_customers:
            return results

        # 定义单个客户的生成任务（带并发限制）
        async def generate_for_single_customer(customer_data: Dict) -> Dict:
            async with semaphore:  # 使用信号量限制并发
                customer_code = customer_data.get("customer_code")
                customer = customer_data.get('customer', {})
                history = customer_data.get('history', [])

                try:
                    # 为单个客户生成话术
                    content = await self.generate_reminder_content(customer, history)
                    return {
                        "customer_code": customer_code,
                        "customer_name": customer.get("name", ""),
                        "content": content,
                        "success": True
                    }
                except Exception as e:
                    logger.error(f"为客户 {customer_code} 生成话术失败: {e}")
                    return {
                        "customer_code": customer_code,
                        "customer_name": customer.get("name", ""),
                        "content": self._generate_default_content(customer),
                        "success": True,
                        "fallback": True
                    }

        try:
            # 并发执行所有客户的话术生成
            tasks = [generate_for_single_customer(c) for c in valid_customers]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            # 处理结果
            for i, result in enumerate(batch_results):
                if isinstance(result, Exception):
                    # 任务出错，使用默认内容
                    customer = valid_customers[i].get('customer', {})
                    results.append({
                        "customer_code": valid_customers[i].get("customer_code"),
                        "customer_name": customer.get("name", ""),
                        "content": self._generate_default_content(customer),
                        "success": True,
                        "fallback": True
                    })
                else:
                    results.append(result)

        except Exception as e:
            logger.error(f"批量生成话术失败: {e}")
            # 出错时为每个客户生成默认内容
            for c in valid_customers:
                customer = c.get('customer', {})
                results.append({
                    "customer_code": c.get("customer_code"),
                    "customer_name": customer.get("name", ""),
                    "content": self._generate_default_content(customer),
                    "success": True
                })

        return results

    def _build_batch_birthday_prompt(self, customers_data: List[Dict]) -> str:
        """构建批量生日回访提示词"""
        today_str = date.today().strftime('%Y年%m月%d日')

        # 构建每个客户的信息摘要
        customers_info = []
        for i, c in enumerate(customers_data, 1):
            customer = c.get('customer', {})
            history = c.get('history', [])

            # 计算年龄
            birthday = customer.get('birthday')
            age = None
            if birthday:
                if isinstance(birthday, str):
                    from datetime import datetime
                    birthday = datetime.strptime(birthday, '%Y-%m-%d').date()
                today = date.today()
                age = today.year - birthday.year
                if (today.month, today.day) < (birthday.month, birthday.day):
                    age -= 1

            # 格式化消费历史（简化版）
            history_summary = "无消费记录"
            if history:
                projects = [h.get('project_name') for h in history if h.get('project_name')]
                products = [h.get('product_name') for h in history if h.get('product_name')]
                items = list(set(projects + products))[:3]
                if items:
                    history_summary = "、".join(items)

            info = f"""
【客户{i}】代码：{c.get('customer_code')}
- 姓名：{customer.get('name', '客户')}
- 性别：{customer.get('gender', '未知')}
- 年龄：{age if age else '未知'}岁
- 会员等级：{customer.get('vip_level', '普通')}
- 累计消费：{customer.get('total_consumption', 0)}元
- 消费偏好：{history_summary}"""
            customers_info.append(info)

        prompt = f"""
你是一位专业的医美机构客户关系专员。今天是{today_str}。

我需要你为以下{len(customers_data)}位即将生日的客户分别生成个性化的生日祝福话术。

客户信息：
{''.join(customers_info)}

请为每位客户生成一段个性化的生日祝福短信，要求：
1. 亲切、真诚、温暖
2. 根据客户的消费偏好和会员等级进行个性化定制
3. 包含生日专属优惠（如生日月8.8折、双倍积分等）
4. 每段话术控制在150-200字
5. 不要使用括号或特殊格式符号

请严格按照以下JSON格式返回（确保是合法的JSON）：
[
  {{"customer_code": "客户代码1", "content": "祝福内容1"}},
  {{"customer_code": "客户代码2", "content": "祝福内容2"}}
]

只返回JSON数组，不要其他任何内容。
"""
        return prompt

    def _parse_batch_response(self, response: str, customers_data: List[Dict]) -> List[Dict]:
        """解析批量生成的响应"""
        results = []

        try:
            # 清理响应中的 markdown 代码块标记
            content = response.strip()
            if content.startswith('```'):
                content = content.split('\n', 1)[1] if '\n' in content else content[3:]
            if content.endswith('```'):
                content = content.rsplit('\n', 1)[0] if '\n' in content else content[:-3]
            content = content.strip()

            # 如果以 json 开头，去掉
            if content.lower().startswith('json'):
                content = content[4:].strip()

            # 解析 JSON
            parsed = json.loads(content)

            if isinstance(parsed, list):
                # 建立客户代码到数据的映射
                customer_map = {c.get('customer_code'): c for c in customers_data}
                response_map = {item.get('customer_code'): item.get('content', '') for item in parsed}

                for c in customers_data:
                    code = c.get('customer_code')
                    customer = c.get('customer', {})
                    content = response_map.get(code, '')

                    if content:
                        results.append({
                            "customer_code": code,
                            "customer_name": customer.get("name", ""),
                            "content": content,
                            "success": True
                        })
                    else:
                        # 该客户没有生成内容，使用默认
                        results.append({
                            "customer_code": code,
                            "customer_name": customer.get("name", ""),
                            "content": self._generate_default_content(customer),
                            "success": True
                        })
            else:
                raise ValueError("响应不是数组格式")

        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"解析批量响应失败: {e}，使用默认内容")
            # 解析失败，为每个客户生成默认内容
            for c in customers_data:
                customer = c.get('customer', {})
                results.append({
                    "customer_code": c.get("customer_code"),
                    "customer_name": customer.get("name", ""),
                    "content": self._generate_default_content(customer),
                    "success": True
                })

        return results

    async def _record_reminder_result(
        self,
        customer: Dict,
        institution_code: str,
        content: str,
        success: bool
    ):
        """记录回访结果"""
        # TODO: 将回访记录保存到数据库
        # 这里可以保存到 pending_birthday_reminder / completed_reminder 表
        logger.info(f"记录回访结果: {customer.get('customer_code')}, 成功: {success}")

    async def get_today_birthday_customers(self, institution_code: str) -> List[Dict]:
        """获取今天生日的客户"""
        await self.data_service.init_connections()
        try:
            return await self.data_service.get_upcoming_birthday_customers(
                institution_code, days_ahead=0
            )
        finally:
            await self.data_service.close_connections()

    async def get_upcoming_birthdays_report(self, institution_code: str, days: int = 7) -> Dict:
        """获取即将生日客户报告"""
        await self.data_service.init_connections()
        try:
            customers = await self.data_service.get_upcoming_birthday_customers(
                institution_code, days_ahead=days
            )

            # 按天数分组
            by_day = {}
            for c in customers:
                day = c.get('days_until_birthday', 0)
                if day not in by_day:
                    by_day[day] = []
                by_day[day].append(c)

            # 统计
            total = len(customers)
            vip_count = len([c for c in customers if c.get('vip_level') in ['GOLD', 'PLATINUM', 'DIAMOND']])

            return {
                'institution_code': institution_code,
                'report_date': date.today().isoformat(),
                'days_ahead': days,
                'total_customers': total,
                'vip_customers': vip_count,
                'by_day': {
                    str(k): {
                        'count': len(v),
                        'customers': [
                            {
                                'customer_code': c.get('customer_code'),
                                'name': c.get('name'),
                                'phone_suffix': c.get('phone', '')[-4:] if c.get('phone') else '',
                                'vip_level': c.get('vip_level'),
                                'total_consumption': c.get('total_consumption', 0),
                                'days_until_birthday': c.get('days_until_birthday', 0)
                            }
                            for c in v
                        ]
                    }
                    for k, v in sorted(by_day.items())
                }
            }
        finally:
            await self.data_service.close_connections()


# 便捷函数
async def run_birthday_reminder_for_all_institutions():
    """为所有机构运行生日回访"""
    service = BirthdayReminderService()
    await service.data_service.init_connections()

    try:
        for institution_code in settings.APP.INSTITUTIONS:
            try:
                await service.run_daily_reminders(institution_code)
            except Exception as e:
                logger.error(f"机构 {institution_code} 回访失败: {e}")
    finally:
        await service.data_service.close_connections()

