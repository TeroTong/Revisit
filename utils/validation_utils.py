"""
验证工具
"""
import re
from datetime import date, datetime
from typing import Any, Optional, Tuple, List
import phonenumbers


class ValidationUtils:
    """验证工具类"""

    @staticmethod
    def validate_phone(phone: str, country: str = 'CN') -> Tuple[bool, str]:
        """验证手机号"""
        try:
            parsed = phonenumbers.parse(phone, country)
            if phonenumbers.is_valid_number(parsed):
                return True, phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
            else:
                return False, "手机号无效"
        except phonenumbers.NumberParseException:
            # 如果phonenumbers解析失败，尝试简单的正则验证
            pattern = r'^1[3-9]\d{9}$'
            if re.match(pattern, phone):
                return True, phone
            else:
                return False, "手机号格式不正确"

    @staticmethod
    def validate_email(email: str) -> Tuple[bool, str]:
        """验证邮箱"""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if re.match(pattern, email):
            return True, email
        else:
            return False, "邮箱格式不正确"

    @staticmethod
    def validate_date(date_str: str, format: str = "%Y-%m-%d") -> Tuple[bool, Optional[date]]:
        """验证日期"""
        try:
            d = datetime.strptime(date_str, format).date()
            return True, d
        except (ValueError, TypeError):
            return False, None

    @staticmethod
    def validate_age(birth_date: date, min_age: int = 0, max_age: int = 150) -> Tuple[bool, int]:
        """验证年龄"""
        today = date.today()
        age = today.year - birth_date.year

        # 调整如果生日还没过
        if (today.month, today.day) < (birth_date.month, birth_date.day):
            age -= 1

        if min_age <= age <= max_age:
            return True, age
        else:
            return False, age

    @staticmethod
    def validate_chinese_name(name: str) -> Tuple[bool, str]:
        """验证中文姓名"""
        # 中文姓名通常为2-4个汉字
        pattern = r'^[\u4e00-\u9fa5]{2,4}$'
        if re.match(pattern, name):
            return True, name
        else:
            return False, "姓名必须是2-4个汉字"

    @staticmethod
    def validate_id_card(id_card: str) -> Tuple[bool, str]:
        """验证身份证号"""
        # 简单的身份证号验证
        pattern = r'^[1-9]\d{5}(18|19|20)\d{2}(0[1-9]|1[0-2])(0[1-9]|[1-2]\d|3[0-1])\d{3}[\dXx]$'
        if re.match(pattern, id_card):
            return True, id_card
        else:
            return False, "身份证号格式不正确"

    @staticmethod
    def validate_required_fields(data: dict, required_fields: List[str]) -> Tuple[bool, List[str]]:
        """验证必填字段"""
        missing_fields = []

        for field in required_fields:
            if field not in data or data[field] is None or data[field] == "":
                missing_fields.append(field)

        if missing_fields:
            return False, missing_fields
        else:
            return True, []

    @staticmethod
    def validate_numeric_range(value: Any, min_val: float = None, max_val: float = None) -> Tuple[bool, str]:
        """验证数值范围"""
        try:
            num = float(value)

            if min_val is not None and num < min_val:
                return False, f"数值不能小于 {min_val}"

            if max_val is not None and num > max_val:
                return False, f"数值不能大于 {max_val}"

            return True, "验证通过"
        except (ValueError, TypeError):
            return False, "不是有效的数值"