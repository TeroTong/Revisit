"""
机构业务模型
"""
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
import json


class Institution(BaseModel):
    """机构业务模型"""

    id: Optional[int] = None
    institution_code: str = Field(..., description="机构代码")
    name: str = Field(..., description="机构名称")
    type: str = Field(..., description="机构类型")
    level: str = Field(..., description="机构等级")
    province: str = Field(..., description="省份")
    city: str = Field(..., description="城市")
    district: str = Field(..., description="区县")
    address: str = Field(..., description="详细地址")
    phone: str = Field(..., description="联系电话")
    email: Optional[str] = Field(None, description="邮箱")
    manager_name: str = Field(..., description="负责人姓名")
    manager_phone: str = Field(..., description="负责人电话")
    is_active: bool = Field(True, description="是否活跃")
    settings: Dict[str, Any] = Field(default_factory=dict, description="设置")

    def get_table_suffix(self) -> str:
        """获取表名后缀"""
        return self.institution_code.lower().replace("-", "_")

    def get_setting(self, key: str, default: Any = None) -> Any:
        """获取设置"""
        return self.settings.get(key, default)

    def update_setting(self, key: str, value: Any):
        """更新设置"""
        self.settings[key] = value

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        data = self.model_dump()
        data['settings'] = json.dumps(self.settings, ensure_ascii=False)
        return data

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'Institution':
        """从数据库行创建Institution实例"""
        data = dict(row)

        # 处理JSON字段
        if 'settings' in data and isinstance(data['settings'], str):
            data['settings'] = json.loads(data['settings'])

        return cls(**data)