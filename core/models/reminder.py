"""
回访业务模型
"""
from datetime import date, datetime
from typing import Optional, List, Dict, Any
from enum import Enum
from pydantic import BaseModel, Field, field_validator
import json


class ReminderType(str, Enum):
    """回访类型"""
    BIRTHDAY = "birthday"
    FESTIVAL = "festival"
    PROJECT_COMPLETION = "project_completion"
    FOLLOW_UP = "follow_up"
    RETENTION = "retention"


class ReminderStatus(str, Enum):
    """回访状态"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ReminderChannel(str, Enum):
    """回访渠道"""
    WECHAT = "wechat"
    SMS = "sms"
    EMAIL = "email"
    PHONE = "phone"


class Reminder(BaseModel):
    """回访业务模型"""

    id: Optional[int] = None
    reminder_type: ReminderType = Field(..., description="回访类型")
    institution_code: str = Field(..., description="机构代码")
    customer_id: int = Field(..., description="客户ID")
    customer_code: str = Field(..., description="客户代码")
    reminder_date: date = Field(..., description="回访日期")
    scheduled_time: Optional[datetime] = Field(None, description="计划发送时间")
    channel: ReminderChannel = Field(ReminderChannel.WECHAT, description="回访渠道")
    status: ReminderStatus = Field(ReminderStatus.PENDING, description="回访状态")
    priority: int = Field(1, description="优先级")
    template_id: Optional[str] = Field(None, description="模板ID")
    content: Optional[str] = Field(None, description="回访内容")
    response: Optional[str] = Field(None, description="客户回复")
    error_message: Optional[str] = Field(None, description="错误信息")
    llm_prompt: Optional[str] = Field(None, description="LLM提示词")
    llm_response: Optional[str] = Field(None, description="LLM响应")
    recommended_projects: List[Dict[str, Any]] = Field(default_factory=list, description="推荐项目")
    recommended_products: List[Dict[str, Any]] = Field(default_factory=list, description="推荐产品")
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @field_validator('priority')
    def validate_priority(cls, v):
        if v < 1 or v > 5:
            raise ValueError('优先级必须在1-5之间')
        return v

    def is_due(self) -> bool:
        """是否到期"""
        today = date.today()
        return self.reminder_date <= today and self.status == ReminderStatus.PENDING

    def mark_processing(self):
        """标记为处理中"""
        self.status = ReminderStatus.PROCESSING
        self.updated_at = datetime.now()

    def mark_completed(self, content: str = None, response: str = None):
        """标记为完成"""
        self.status = ReminderStatus.COMPLETED
        if content:
            self.content = content
        if response:
            self.response = response
        self.updated_at = datetime.now()

    def mark_failed(self, error_message: str):
        """标记为失败"""
        self.status = ReminderStatus.FAILED
        self.error_message = error_message
        self.updated_at = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        data = self.model_dump()

        # 处理日期时间字段
        date_fields = ['reminder_date', 'scheduled_time', 'created_at', 'updated_at']
        for field in date_fields:
            if field in data and data[field]:
                if isinstance(data[field], date):
                    data[field] = data[field].isoformat()
                elif isinstance(data[field], datetime):
                    data[field] = data[field].isoformat()

        # 处理JSON字段
        json_fields = ['recommended_projects', 'recommended_products']
        for field in json_fields:
            if field in data:
                data[field] = json.dumps(data[field], ensure_ascii=False)

        return data

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'Reminder':
        """从数据库行创建Reminder实例"""
        data = dict(row)

        # 处理JSON字段
        json_fields = ['recommended_projects', 'recommended_products']
        for field in json_fields:
            if field in data and isinstance(data[field], str):
                data[field] = json.loads(data[field])

        # 处理类型转换
        if 'reminder_type' in data:
            data['reminder_type'] = ReminderType(data['reminder_type'])
        if 'status' in data:
            data['status'] = ReminderStatus(data['status'])
        if 'channel' in data:
            data['channel'] = ReminderChannel(data['channel'])

        return cls(**data)