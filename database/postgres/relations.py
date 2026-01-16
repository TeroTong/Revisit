# database/postgres/relations.py
"""
医美关系查询工具
"""

import asyncpg
import logging

logger = logging.getLogger(__name__)


async def get_project_upgrades(conn: asyncpg.Connection, project_code: str):
    """获取项目的升级选项"""

    # 首先获取项目ID
    project = await conn.fetchrow('''
        SELECT project_id FROM project WHERE project_code = $1
    ''', project_code)

    if not project:
        return []

    # 查询升级关系
    upgrades = await conn.fetch('''
        SELECT 
            p.project_code,
            p.name,
            p.category,
            p.description,
            mr.relation_level,
            mr.description as relation_description
        FROM medical_relation mr
        JOIN project p ON mr.target_type = 'PROJECT' AND mr.target_id = p.project_id
        WHERE mr.source_type = 'PROJECT'
          AND mr.source_id = $1
          AND mr.relation_type = 'UPGRADE'
        ORDER BY mr.relation_level DESC
    ''', project['project_id'])

    return upgrades


async def get_similar_items(conn: asyncpg.Connection, item_type: str, item_code: str):
    """获取相似的项目或产品"""

    # 首先获取项目/产品ID
    if item_type == 'PROJECT':
        item = await conn.fetchrow('''
            SELECT project_id FROM project WHERE project_code = $1
        ''', item_code)
        table = 'project'
        id_field = 'project_id'
    else:
        item = await conn.fetchrow('''
            SELECT product_id FROM product WHERE product_code = $1
        ''', item_code)
        table = 'product'
        id_field = 'product_id'

    if not item:
        return []

    item_id = item[id_field]

    # 查询相似关系（包括双向关系）
    similar_items = await conn.fetch(f'''
        SELECT 
            mr.target_type,
            CASE 
                WHEN mr.target_type = 'PROJECT' THEN p.project_code
                WHEN mr.target_type = 'PRODUCT' THEN pd.product_code
            END as code,
            CASE 
                WHEN mr.target_type = 'PROJECT' THEN p.name
                WHEN mr.target_type = 'PRODUCT' THEN pd.name
            END as name,
            CASE 
                WHEN mr.target_type = 'PROJECT' THEN p.description
                WHEN mr.target_type = 'PRODUCT' THEN pd.description
            END as description,
            mr.relation_level,
            mr.description as relation_description
        FROM medical_relation mr
        LEFT JOIN project p ON mr.target_type = 'PROJECT' AND mr.target_id = p.project_id
        LEFT JOIN product pd ON mr.target_type = 'PRODUCT' AND mr.target_id = pd.product_id
        WHERE mr.source_type = $1
          AND mr.source_id = $2
          AND mr.relation_type = 'SIMILAR'

        UNION ALL

        -- 反向关系（如果是双向的）
        SELECT 
            mr.source_type,
            CASE 
                WHEN mr.source_type = 'PROJECT' THEN p.project_code
                WHEN mr.source_type = 'PRODUCT' THEN pd.product_code
            END as code,
            CASE 
                WHEN mr.source_type = 'PROJECT' THEN p.name
                WHEN mr.source_type = 'PRODUCT' THEN pd.name
            END as name,
            CASE 
                WHEN mr.source_type = 'PROJECT' THEN p.description
                WHEN mr.source_type = 'PRODUCT' THEN pd.description
            END as description,
            mr.relation_level,
            mr.description as relation_description
        FROM medical_relation mr
        LEFT JOIN project p ON mr.source_type = 'PROJECT' AND mr.source_id = p.project_id
        LEFT JOIN product pd ON mr.source_type = 'PRODUCT' AND mr.source_id = pd.product_id
        WHERE mr.target_type = $1
          AND mr.target_id = $2
          AND mr.relation_type = 'SIMILAR'
          AND mr.is_bidirectional = TRUE

        ORDER BY relation_level DESC
    ''', item_type, item_id)

    return similar_items


async def get_related_items(conn: asyncpg.Connection, item_type: str, item_code: str):
    """获取所有相关的项目或产品"""

    # 首先获取项目/产品ID
    if item_type == 'PROJECT':
        item = await conn.fetchrow('''
            SELECT project_id FROM project WHERE project_code = $1
        ''', item_code)
        table = 'project'
        id_field = 'project_id'
    else:
        item = await conn.fetchrow('''
            SELECT product_id FROM product WHERE product_code = $1
        ''', item_code)
        table = 'product'
        id_field = 'product_id'

    if not item:
        return []

    item_id = item[id_field]

    # 查询所有关系
    relations = await conn.fetch(f'''
        SELECT 
            mr.relation_type,
            mr.target_type,
            CASE 
                WHEN mr.target_type = 'PROJECT' THEN p.project_code
                WHEN mr.target_type = 'PRODUCT' THEN pd.product_code
            END as target_code,
            CASE 
                WHEN mr.target_type = 'PROJECT' THEN p.name
                WHEN mr.target_type = 'PRODUCT' THEN pd.name
            END as target_name,
            mr.relation_level,
            mr.description as relation_description
        FROM medical_relation mr
        LEFT JOIN project p ON mr.target_type = 'PROJECT' AND mr.target_id = p.project_id
        LEFT JOIN product pd ON mr.target_type = 'PRODUCT' AND mr.target_id = pd.product_id
        WHERE mr.source_type = $1
          AND mr.source_id = $2
        ORDER BY mr.relation_type, mr.relation_level DESC
    ''', item_type, item_id)

    # 按关系类型分组
    result = {}
    for rel in relations:
        rel_type = rel['relation_type']
        if rel_type not in result:
            result[rel_type] = []

        result[rel_type].append({
            'target_type': rel['target_type'],
            'target_code': rel['target_code'],
            'target_name': rel['target_name'],
            'relation_level': rel['relation_level'],
            'relation_description': rel['relation_description']
        })

    return result


async def add_medical_relation(
        conn: asyncpg.Connection,
        source_type: str,
        source_code: str,
        target_type: str,
        target_code: str,
        relation_type: str,
        description: str = None,
        relation_level: int = 1,
        is_bidirectional: bool = False
):
    """添加医美关系"""

    # 获取源ID
    if source_type == 'PROJECT':
        source = await conn.fetchrow('''
            SELECT project_id FROM project WHERE project_code = $1
        ''', source_code)
        source_id = source['project_id'] if source else None
    else:
        source = await conn.fetchrow('''
            SELECT product_id FROM product WHERE product_code = $1
        ''', source_code)
        source_id = source['product_id'] if source else None

    # 获取目标ID
    if target_type == 'PROJECT':
        target = await conn.fetchrow('''
            SELECT project_id FROM project WHERE project_code = $1
        ''', target_code)
        target_id = target['project_id'] if target else None
    else:
        target = await conn.fetchrow('''
            SELECT product_id FROM product WHERE product_code = $1
        ''', target_code)
        target_id = target['product_id'] if target else None

    if not source_id or not target_id:
        raise ValueError("源或目标项目/产品不存在")

    # 插入关系
    await conn.execute('''
        INSERT INTO medical_relation 
        (source_type, source_id, target_type, target_id, relation_type, 
         relation_level, description, is_bidirectional)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (source_type, source_id, target_type, target_id, relation_type) 
        DO UPDATE SET
        relation_level = EXCLUDED.relation_level,
        description = EXCLUDED.description,
        is_bidirectional = EXCLUDED.is_bidirectional,
        updated_at = CURRENT_TIMESTAMP
    ''', source_type, source_id, target_type, target_id, relation_type,
                       relation_level, description, is_bidirectional)

    return True