"""
日期时间工具
"""
from datetime import datetime, date, timedelta
from typing import Optional, Tuple


class DateUtils:
    """日期工具类"""

    @staticmethod
    def parse_date(date_str: str, format: str = "%Y-%m-%d") -> Optional[date]:
        """解析日期字符串"""
        try:
            return datetime.strptime(date_str, format).date()
        except (ValueError, TypeError):
            return None

    @staticmethod
    def format_date(d: date, format: str = "%Y-%m-%d") -> str:
        """格式化日期"""
        return d.strftime(format) if d else ""

    @staticmethod
    def parse_datetime(datetime_str: str, format: str = "%Y-%m-%d %H:%M:%S") -> Optional[datetime]:
        """解析日期时间字符串"""
        try:
            return datetime.strptime(datetime_str, format)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def format_datetime(dt: datetime, format: str = "%Y-%m-%d %H:%M:%S") -> str:
        """格式化日期时间"""
        return dt.strftime(format) if dt else ""

    @staticmethod
    def get_date_range(days: int = 30, end_date: date = None) -> Tuple[date, date]:
        """获取日期范围"""
        if end_date is None:
            end_date = date.today()

        start_date = end_date - timedelta(days=days)
        return start_date, end_date

    @staticmethod
    def get_age(birth_date: date, reference_date: date = None) -> int:
        """计算年龄"""
        if reference_date is None:
            reference_date = date.today()

        age = reference_date.year - birth_date.year

        # 如果生日还没过，年龄减1
        if (reference_date.month, reference_date.day) < (birth_date.month, birth_date.day):
            age -= 1

        return age

    @staticmethod
    def is_birthday_today(birth_date: date, today: date = None) -> bool:
        """今天是否是生日"""
        if today is None:
            today = date.today()

        return today.month == birth_date.month and today.day == birth_date.day

    @staticmethod
    def days_until_birthday(birth_date: date, today: date = None) -> int:
        """距离生日的天数"""
        if today is None:
            today = date.today()

        # 今年的生日
        birthday_this_year = date(today.year, birth_date.month, birth_date.day)

        if birthday_this_year >= today:
            return (birthday_this_year - today).days
        else:
            # 生日已过，计算明年的生日
            birthday_next_year = date(today.year + 1, birth_date.month, birth_date.day)
            return (birthday_next_year - today).days

    @staticmethod
    def get_business_date(d: date = None) -> date:
        """获取营业日期（如果是周末则调整为周五）"""
        if d is None:
            d = date.today()

        # 0=周一, 6=周日
        weekday = d.weekday()

        if weekday >= 5:  # 周六或周日
            # 调整为周五
            d = d - timedelta(days=weekday - 4)

        return d