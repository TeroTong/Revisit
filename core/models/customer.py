"""
客户业务模型
"""
from datetime import date, datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, field_validator
import json


class Customer(BaseModel):
    """客户业务模型"""

    id: Optional[int] = None
    person_id: str = Field(..., description="自然人ID")
    name: str = Field(..., description="姓名")
    gender: str = Field(..., description="性别")
    birthday: date = Field(..., description="生日")
    phone: str = Field(..., description="手机号")
    email: Optional[str] = Field(None, description="邮箱")
    wechat_id: Optional[str] = Field(None, description="微信ID")
    institution_code: str = Field(..., description="机构代码")
    customer_code: str = Field(..., description="机构客户代码")
    level: str = Field("普通会员", description="会员等级")
    points: int = Field(0, description="积分")
    total_consumption: float = Field(0.0, description="累计消费")
    first_visit_date: Optional[date] = Field(None, description="首次到访日期")
    last_visit_date: Optional[date] = Field(None, description="最后到访日期")
    visit_count: int = Field(0, description="到访次数")
    is_vip: bool = Field(False, description="是否是VIP")
    vip_start_date: Optional[date] = Field(None, description="VIP开始日期")
    vip_end_date: Optional[date] = Field(None, description="VIP结束日期")
    tags: List[str] = Field(default_factory=list, description="标签")
    preferences: Dict[str, Any] = Field(default_factory=dict, description="偏好")
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @field_validator('gender')
    def validate_gender(cls, v):
        if v not in ['男', '女', '其他']:
            raise ValueError('性别必须是"男", "女"或"其他"')
        return v

    @field_validator('phone')
    def validate_phone(cls, v):
        # 简单的手机号验证
        if not v or len(v) < 11:
            raise ValueError('手机号格式不正确')
        return v

    def get_age(self) -> int:
        """计算年龄"""
        today = date.today()
        age = today.year - self.birthday.year

        # 如果生日还没过，年龄减1
        if (today.month, today.day) < (self.birthday.month, self.birthday.day):
            age -= 1

        return age

    def get_birthday_this_year(self) -> date:
        """获取今年的生日日期"""
        return date(date.today().year, self.birthday.month, self.birthday.day)

    def is_birthday_today(self) -> bool:
        """今天是否是生日"""
        today = date.today()
        return today.month == self.birthday.month and today.day == self.birthday.day

    def days_until_birthday(self) -> int:
        """距离生日的天数"""
        today = date.today()
        birthday_this_year = self.get_birthday_this_year()

        if birthday_this_year >= today:
            return (birthday_this_year - today).days
        else:
            # 生日已过，计算明年的生日
            birthday_next_year = date(today.year + 1, self.birthday.month, self.birthday.day)
            return (birthday_next_year - today).days

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        data = self.model_dump()
        data['birthday'] = self.birthday.isoformat() if self.birthday else None
        data['first_visit_date'] = self.first_visit_date.isoformat() if self.first_visit_date else None
        data['last_visit_date'] = self.last_visit_date.isoformat() if self.last_visit_date else None
        data['vip_start_date'] = self.vip_start_date.isoformat() if self.vip_start_date else None
        data['vip_end_date'] = self.vip_end_date.isoformat() if self.vip_end_date else None
        data['created_at'] = self.created_at.isoformat() if self.created_at else None
        data['updated_at'] = self.updated_at.isoformat() if self.updated_at else None
        return data

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'Customer':
        """从数据库行创建Customer实例"""
        data = dict(row)

        # 处理JSON字段
        if 'tags' in data and isinstance(data['tags'], str):
            data['tags'] = json.loads(data['tags'])

        if 'preferences' in data and isinstance(data['preferences'], str):
            data['preferences'] = json.loads(data['preferences'])

        return cls(**data)