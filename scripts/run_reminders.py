"""
生日回访运行脚本

用法：
    python -m scripts.run_reminders                        # 运行所有机构的回访
    python -m scripts.run_reminders --institution BJ-HA-001  # 指定机构
    python -m scripts.run_reminders --report               # 查看即将生日的客户报告
    python -m scripts.run_reminders --test                 # 测试模式（不发送消息）
"""

import os

# 禁用代理设置（解决远程数据库连接问题）
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    os.environ.pop(key, None)

import asyncio
import argparse
import logging
import sys
import json
from pathlib import Path
from datetime import date

# 添加项目根目录
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings
from services.birthday_reminder import BirthdayReminderService
from services.data_sync import DataSyncService
from utils.logger import setup_logger

logger = setup_logger(__name__)


class ReminderRunner:
    """回访任务运行器"""

    def __init__(self, test_mode: bool = False):
        self.test_mode = test_mode
        self.service = BirthdayReminderService()
        self.data_service = DataSyncService()

    async def init(self):
        """初始化"""
        await self.data_service.init_connections()

    async def close(self):
        """关闭"""
        await self.data_service.close_connections()

    async def run_reminders(self, institution_code: str = None):
        """运行回访任务"""
        institutions = [institution_code] if institution_code else settings.APP.INSTITUTIONS

        print("\n" + "=" * 60)
        print("  医美客户回访系统 - 生日回访任务")
        print("=" * 60)
        print(f"  日期: {date.today()}")
        print(f"  提前天数: {settings.APP.BIRTHDAY_REMINDER_DAYS_AHEAD}")
        print(f"  机构列表: {', '.join(institutions)}")
        print(f"  测试模式: {'是' if self.test_mode else '否'}")
        print("=" * 60)

        for inst_code in institutions:
            print(f"\n[机构] {inst_code}")
            print("-" * 40)

            try:
                if self.test_mode:
                    await self._run_test_mode(inst_code)
                else:
                    await self.service.run_daily_reminders(inst_code)
            except Exception as e:
                logger.error(f"机构 {inst_code} 回访失败: {e}")

        print("\n" + "=" * 60)
        print("  回访任务完成")
        print("=" * 60)

    async def _run_test_mode(self, institution_code: str):
        """测试模式：只显示要发送的内容，不实际发送"""
        days_ahead = settings.APP.BIRTHDAY_REMINDER_DAYS_AHEAD
        customers = await self.data_service.get_upcoming_birthday_customers(
            institution_code, days_ahead
        )

        print(f"  找到 {len(customers)} 个即将生日的客户")

        for customer in customers[:3]:  # 测试模式只处理前3个
            customer_code = customer.get('customer_code')
            customer_name = customer.get('name')
            days_until = customer.get('days_until_birthday', 0)

            print(f"\n  [客户] {customer_code}: {customer_name} ({days_until}天后生日)")

            # 获取消费历史
            history = await self.data_service.get_customer_consumption_history(
                customer.get('institution_customer_id'),
                institution_code,
                limit=5
            )

            # 生成回访内容
            content = await self.service.generate_reminder_content(customer, history)

            print(f"  [生成内容]")
            print("  " + "-" * 36)
            for line in content.split('\n'):
                print(f"  {line}")
            print("  " + "-" * 36)

        if len(customers) > 3:
            print(f"\n  ... 还有 {len(customers) - 3} 个客户未显示（测试模式限制）")

    async def show_report(self, institution_code: str = None, days: int = 7):
        """显示即将生日客户报告"""
        institutions = [institution_code] if institution_code else settings.APP.INSTITUTIONS

        print("\n" + "=" * 60)
        print("  医美客户回访系统 - 即将生日客户报告")
        print("=" * 60)
        print(f"  日期: {date.today()}")
        print(f"  统计天数: {days}")
        print("=" * 60)

        for inst_code in institutions:
            report = await self.service.get_upcoming_birthdays_report(inst_code, days)

            print(f"\n[机构] {inst_code}")
            print("-" * 40)
            print(f"  总客户数: {report['total_customers']}")
            print(f"  VIP客户: {report['vip_customers']}")

            if report['total_customers'] > 0:
                print("\n  按天分布:")
                for day, data in report['by_day'].items():
                    day_label = "今天" if day == "0" else f"{day}天后"
                    print(f"\n  [{day_label}] {data['count']} 人")
                    for c in data['customers'][:5]:  # 每天最多显示5人
                        print(f"    - {c['code']}: {c['name']} ({c['vip_level']}) ****{c['phone']}")

        print("\n" + "=" * 60)


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="医美客户回访系统 - 生日回访运行工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python -m scripts.run_reminders                        # 运行所有机构
  python -m scripts.run_reminders --institution BJ-HA-001  # 指定机构
  python -m scripts.run_reminders --report               # 查看报告
  python -m scripts.run_reminders --test                 # 测试模式
        """
    )
    parser.add_argument(
        '--institution', '-i',
        type=str,
        help='指定机构代码'
    )
    parser.add_argument(
        '--report', '-r',
        action='store_true',
        help='显示即将生日客户报告'
    )
    parser.add_argument(
        '--test', '-t',
        action='store_true',
        help='测试模式（不发送消息）'
    )
    parser.add_argument(
        '--days', '-d',
        type=int,
        default=7,
        help='报告统计天数（默认7天）'
    )

    args = parser.parse_args()

    runner = ReminderRunner(test_mode=args.test)

    try:
        await runner.init()

        if args.report:
            await runner.show_report(args.institution, args.days)
        else:
            await runner.run_reminders(args.institution)

    except KeyboardInterrupt:
        print("\n\n⚠️ 用户中断")
    except Exception as e:
        logger.error(f"❌ 运行失败: {e}")
        raise
    finally:
        await runner.close()


if __name__ == "__main__":
    asyncio.run(main())

