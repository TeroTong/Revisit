"""
项目业务模型
"""
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, field_validator
import json


class Project(BaseModel):
    """医美项目业务模型"""

    id: Optional[int] = None
    project_code: str = Field(..., description="项目代码")
    name: str = Field(..., description="项目名称")
    category: str = Field(..., description="项目类别")
    subcategory: Optional[str] = Field(None, description="子类别")
    description: Optional[str] = Field(None, description="项目描述")
    duration: Optional[int] = Field(None, description="项目时长（分钟）")
    price: float = Field(..., description="价格")
    min_price: Optional[float] = Field(None, description="最低价格")
    max_price: Optional[float] = Field(None, description="最高价格")
    suitable_for: List[str] = Field(default_factory=list, description="适用人群")
    contraindications: List[str] = Field(default_factory=list, description="禁忌症")
    recovery_time: Optional[str] = Field(None, description="恢复时间")
    effect_duration: Optional[str] = Field(None, description="效果持续时间")
    is_active: bool = Field(True, description="是否有效")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="元数据")

    @field_validator('price')
    def validate_price(cls, v):
        if v < 0:
            raise ValueError('价格不能为负数')
        return v

    def get_price_range(self) -> str:
        """获取价格范围"""
        if self.min_price and self.max_price:
            return f"{self.min_price} - {self.max_price}"
        return str(self.price)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        data = self.model_dump()
        data['suitable_for'] = json.dumps(self.suitable_for, ensure_ascii=False)
        data['contraindications'] = json.dumps(self.contraindications, ensure_ascii=False)
        data['metadata'] = json.dumps(self.metadata, ensure_ascii=False)
        return data

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'Project':
        """从数据库行创建Project实例"""
        data = dict(row)

        # 处理JSON字段
        json_fields = ['suitable_for', 'contraindications', 'metadata']
        for field in json_fields:
            if field in data and isinstance(data[field], str):
                data[field] = json.loads(data[field])

        return cls(**data)