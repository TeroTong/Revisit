# scripts/test_relations.py
"""
测试医美关系功能
"""

import asyncio
import sys
import logging
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root.absolute()))

from config.settings import settings
import asyncpg
from database.postgres.relations import (
    get_project_upgrades, get_similar_items,
    get_related_items, add_medical_relation
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_relations():
    """测试医美关系功能"""

    conn = await asyncpg.connect(
        host=settings.DATABASE.POSTGRES_HOST,
        port=settings.DATABASE.POSTGRES_PORT,
        user=settings.DATABASE.POSTGRES_USER,
        password=settings.DATABASE.POSTGRES_PASSWORD,
        database=settings.DATABASE.POSTGRES_DB
    )

    try:
        logger.info("🧪 测试医美关系功能...")

        # 1. 测试获取项目升级选项
        logger.info("\n1. 测试获取项目升级选项:")
        upgrades = await get_project_upgrades(conn, 'BOTOX-001')
        for upgrade in upgrades:
            logger.info(f"  升级到: {upgrade['name']} ")

        # 2. 测试获取相似项目
        logger.info("\n2. 测试获取相似项目:")
        similar = await get_similar_items(conn, 'PROJECT', 'FILLER-001')
        for item in similar:
            logger.info(f"  相似: {item['name']} (相似度: {item['relation_level']}/5)")

        # 3. 测试获取所有相关项目
        logger.info("\n3. 测试获取所有相关项目:")
        related = await get_related_items(conn, 'PROJECT', 'LASER-001')
        for rel_type, items in related.items():
            logger.info(f"  {rel_type} 关系:")
            for item in items:
                logger.info(f"    - {item['target_name']}")

        # 4. 测试添加新关系
        logger.info("\n4. 测试添加新关系:")
        success = await add_medical_relation(
            conn,
            source_type='PROJECT',
            source_code='THERMAGE-001',
            target_type='PROJECT',
            target_code='ULTHERA-001',
            relation_type='SIMILAR',
            description='两种紧肤技术，原理不同但效果类似',
            relation_level=4,
            is_bidirectional=True
        )

        if success:
            logger.info("✅ 成功添加新关系")

        # 5. 验证表结构
        logger.info("\n5. 验证表结构:")
        columns = await conn.fetch('''
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'medical_relation'
            ORDER BY ordinal_position
        ''')

        logger.info(f"📋 medical_relation 表有 {len(columns)} 列:")
        for col in columns:
            logger.info(f"  - {col['column_name']} ({col['data_type']})")

        # 6. 统计关系数据
        logger.info("\n6. 关系数据统计:")
        stats = await conn.fetch('''
            SELECT 
                relation_type,
                COUNT(*) as count,
                COUNT(DISTINCT source_type) as source_types,
                COUNT(DISTINCT target_type) as target_types
            FROM medical_relation
            GROUP BY relation_type
            ORDER BY count DESC
        ''')

        for stat in stats:
            logger.info(f"  {stat['relation_type']}: {stat['count']} 条记录")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(test_relations())