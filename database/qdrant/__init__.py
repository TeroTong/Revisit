# database/qdrant/__init__.py
"""
Qdrant向量数据库模块
包含连接管理、Schema管理等功能
"""

import logging

# 配置日志
logger = logging.getLogger(__name__)

# 导出连接管理相关
from .connection import (
    QdrantConnection,
    get_qdrant_client,
    test_qdrant_connection,
    test_qdrant_connection_simple,
    close_qdrant_connection,
    get_qdrant_health,
    async_health_check,
    wait_for_qdrant,
    init_qdrant_connection
    # 注意：execute_query_with_retry 是 QdrantConnection 类的方法，不是直接导出的函数
)

# 导出Schema管理相关
from .schema import (
    QdrantSchemaManager,
    init_qdrant_schema,
    get_qdrant_collections_info,
    check_qdrant_collections_health
)

# 检查兼容版本是否存在
try:
    from .schema_compatible import (
        CompatibleQdrantSchemaManager,
        init_qdrant_schema_compatible
    )

    HAS_COMPATIBLE_SCHEMA = True
except ImportError:
    HAS_COMPATIBLE_SCHEMA = False
    CompatibleQdrantSchemaManager = None
    init_qdrant_schema_compatible = None

# 所有导出的公共接口
__all__ = [
    # 连接管理
    'QdrantConnection',
    'get_qdrant_client',
    'test_qdrant_connection',
    'test_qdrant_connection_simple',
    'close_qdrant_connection',
    'get_qdrant_health',
    'async_health_check',
    'wait_for_qdrant',
    'init_qdrant_connection',

    # Schema管理
    'QdrantSchemaManager',
    'init_qdrant_schema',
    'get_qdrant_collections_info',
    'check_qdrant_collections_health',

    # 版本信息
    'HAS_COMPATIBLE_SCHEMA',
]

# 如果存在兼容版本，也添加到__all__
if HAS_COMPATIBLE_SCHEMA:
    __all__.extend([
        'CompatibleQdrantSchemaManager',
        'init_qdrant_schema_compatible'
    ])

# 版本信息
__version__ = "1.0.0"
__author__ = "医美客户回访系统"
__description__ = "Qdrant向量数据库集成模块"