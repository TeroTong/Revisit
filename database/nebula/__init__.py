"""
NebulaGraph模块初始化
"""
from database.nebula.connection import (
    get_nebula_session,
    close_nebula_session,
    get_nebula_session_async,
    close_nebula_connections,
    execute_ngql,
    test_nebula_connection,
    initialize_nebula_graph
)
from database.nebula.schema import (
    NebulaSchemaManager,
    create_tags,
    create_edges,
    init_nebula_schema
)

# 注意：新的connection.py中没有全局的session变量
# 使用get_nebula_session()函数来获取会话

__all__ = [
    # 连接相关函数
    'get_nebula_session',
    'get_nebula_session_async',
    'close_nebula_session',
    'close_nebula_connections',
    'execute_ngql',
    'test_nebula_connection',

    # 初始化函数
    'initialize_nebula_graph',

    # Schema相关
    'NebulaSchemaManager',
    'create_tags',
    'create_edges',
    'init_nebula_schema'
]

# 初始化说明
"""
使用示例：
1. 获取会话：
    from database.nebula import get_nebula_session
    session = get_nebula_session()
    
2. 执行查询：
    from database.nebula import execute_ngql
    result = execute_ngql("SHOW HOSTS")
    
3. 初始化集群：
    from database.nebula import initialize_nebula_graph
    initialize_nebula_graph("your_space_name")
    
4. 测试连接：
    from database.nebula import test_nebula_connection
    test_nebula_connection()
"""