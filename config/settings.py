"""
应用配置管理
"""
from pathlib import Path
from typing import List, Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings

# 项目根目录
BASE_DIR = Path(__file__).parent.parent


class DatabaseConfig(BaseSettings):
    """数据库配置"""

    # PostgreSQL
    POSTGRES_HOST: str = Field("localhost", env="POSTGRES_HOST")
    POSTGRES_PORT: int = Field(5432, env="POSTGRES_PORT")
    POSTGRES_USER: str = Field("postgres", env="POSTGRES_USER")
    POSTGRES_PASSWORD: str = Field("lancy", env="POSTGRES_PASSWORD")
    POSTGRES_DB: str = Field("revisit", env="POSTGRES_DB")
    POSTGRES_POOL_MIN_SIZE: int = Field(5, env="POSTGRES_POOL_MIN_SIZE")
    POSTGRES_POOL_MAX_SIZE: int = Field(20, env="POSTGRES_POOL_MAX_SIZE")

    # NebulaGraph
    NEBULA_HOST: str = Field("localhost", env="NEBULA_HOST")
    NEBULA_PORT: int = Field(9669, env="NEBULA_PORT")
    NEBULA_USER: str = Field("root", env="NEBULA_USER")
    NEBULA_PASSWORD: str = Field("nebula", env="NEBULA_PASSWORD")
    NEBULA_SPACE: str = Field("revisit", env="NEBULA_SPACE")

    # 新增：控制是否自动添加主机
    NEBULA_AUTO_ADD_HOSTS: bool = Field(False, env='NEBULA_AUTO_ADD_HOSTS')  # 根据你的docker-compose配置设置

    # ClickHouse
    CLICKHOUSE_HOST: str = Field("localhost", env="CLICKHOUSE_HOST")
    CLICKHOUSE_PORT: int = Field(8123, env="CLICKHOUSE_PORT")
    CLICKHOUSE_USER: str = Field("default", env="CLICKHOUSE_USER")
    CLICKHOUSE_PASSWORD: str = Field("", env="CLICKHOUSE_PASSWORD")
    CLICKHOUSE_DB: str = Field("revisit", env="CLICKHOUSE_DB")

    # Qdrant
    QDRANT_HOST: str = Field("localhost", env="QDRANT_HOST")
    QDRANT_PORT: int = Field(6333, env="QDRANT_PORT")
    QDRANT_API_KEY: Optional[str] = Field(None, env="QDRANT_API_KEY")
    QDRANT_COLLECTIONS: List[str] = Field(default=["customer_profiles", "medical_knowledge", "consultation_patterns"], env="QDRANT_COLLECTIONS")

    @property
    def postgres_dsn(self) -> str:
        """PostgreSQL连接字符串"""
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def postgres_async_dsn(self) -> str:
        """PostgreSQL异步连接字符串"""
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    class Config:
        env_file = ".env"


class LLMConfig(BaseSettings):
    """LLM配置"""

    OPENAI_API_KEY: str = Field("sk-0f09f31732564f60aecdda5c8bed0431", env="OPENAI_API_KEY")
    OPENAI_API_BASE: str = Field("https://dashscope.aliyuncs.com/compatible-mode/v1", env="OPENAI_API_BASE")
    OPENAI_MODEL: str = Field("deepseek-r1-distill-qwen-1.5b", env="OPENAI_MODEL")
    OPENAI_MAX_TOKENS: int = Field(1000, env="OPENAI_MAX_TOKENS")
    OPENAI_TEMPERATURE: float = Field(0.7, env="OPENAI_TEMPERATURE")

    class Config:
        env_file = ".env"


class AppConfig(BaseSettings):
    """应用配置"""

    APP_NAME: str = Field("医美客户回访系统", env="APP_NAME")
    APP_VERSION: str = Field("1.0.0", env="APP_VERSION")
    DEBUG: bool = Field(False, env="DEBUG")
    ENVIRONMENT: str = Field("development", env="ENVIRONMENT")

    # 服务器配置
    HOST: str = Field("0.0.0.0", env="HOST")
    PORT: int = Field(8000, env="PORT")

    # 路径配置
    DATA_DIR: Path = Field(BASE_DIR / "data", env="DATA_DIR")
    LOG_DIR: Path = Field(BASE_DIR / "logs", env="LOG_DIR")

    # 日志配置
    LOG_LEVEL: str = Field("INFO", env="LOG_LEVEL")
    LOG_FORMAT: str = Field(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        env="LOG_FORMAT"
    )

    # 回访配置
    BIRTHDAY_REMINDER_DAYS_AHEAD: int = Field(7, env="BIRTHDAY_REMINDER_DAYS_AHEAD")
    REMINDER_BATCH_SIZE: int = Field(100, env="REMINDER_BATCH_SIZE")
    MAX_RETRY_COUNT: int = Field(3, env="MAX_RETRY_COUNT")

    # 机构列表
    INSTITUTIONS: List[str] = Field(["BJ-HA-001", "SH-ML-002"], env="INSTITUTIONS")

    # 任务调度
    SCHEDULER_ENABLED: bool = Field(True, env="SCHEDULER_ENABLED")

    @field_validator("DATA_DIR", "LOG_DIR", mode='before')
    def validate_dirs(cls, v):
        """验证目录"""
        path = Path(v)
        path.mkdir(parents=True, exist_ok=True)
        return path

    @field_validator("INSTITUTIONS", mode='before')
    def parse_institutions(cls, v):
        """解析机构列表"""
        if isinstance(v, str):
            return [inst.strip() for inst in v.split(",")]
        return v

    class Config:
        env_file = ".env"


class NotificationConfig(BaseSettings):
    """通知配置"""

    # 微信配置
    WECHAT_APP_ID: Optional[str] = Field(None, env="WECHAT_APP_ID")
    WECHAT_APP_SECRET: Optional[str] = Field(None, env="WECHAT_APP_SECRET")
    WECHAT_TEMPLATE_ID: Optional[str] = Field(None, env="WECHAT_TEMPLATE_ID")

    # 短信配置
    SMS_ENABLED: bool = Field(False, env="SMS_ENABLED")
    ALIYUN_ACCESS_KEY_ID: Optional[str] = Field(None, env="ALIYUN_ACCESS_KEY_ID")
    ALIYUN_ACCESS_KEY_SECRET: Optional[str] = Field(None, env="ALIYUN_ACCESS_KEY_SECRET")

    class Config:
        env_file = ".env"


class Settings(BaseSettings):
    """全局设置"""

    DATABASE: DatabaseConfig = DatabaseConfig()
    LLM: LLMConfig = LLMConfig()
    APP: AppConfig = AppConfig()
    NOTIFICATION: NotificationConfig = NotificationConfig()

    class Config:
        env_file = ".env"
        env_nested_delimiter = "__"


# 全局配置实例
settings = Settings()