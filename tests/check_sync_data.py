"""检查数据是否同步到各个数据库"""
import os
import sys

# 设置输出编码
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# 禁用代理
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    os.environ.pop(key, None)

import asyncio

async def check_clickhouse():
    """检查 ClickHouse 数据"""
    print('=' * 60)
    print('检查 ClickHouse 数据')
    print('=' * 60)

    try:
        from database.clickhouse.connection import ClickHouseConnection

        client = await ClickHouseConnection.get_client()

        tables = [
            'dim_institution',
            'dim_doctor',
            'dim_project',
            'dim_product',
            'dim_customer',
            'fact_consumption',
        ]

        for table in tables:
            try:
                result = await client.fetch(f'SELECT COUNT(*) as cnt FROM {table}')
                count = result[0]['cnt'] if result else 0
                status = '[OK]' if count > 0 else '[EMPTY]'
                print(f'  {status} {table}: {count} 条')
            except Exception as e:
                print(f'  [ERROR] {table}: 错误 - {str(e)[:50]}')

        await ClickHouseConnection.close_client()
        return True
    except Exception as e:
        print(f'  [ERROR] ClickHouse 连接失败: {e}')
        return False


def check_qdrant():
    """检查 Qdrant 数据"""
    print('\n' + '=' * 60)
    print('检查 Qdrant 数据')
    print('=' * 60)

    try:
        from qdrant_client import QdrantClient
        from config.settings import settings

        client = QdrantClient(
            host=settings.DATABASE.QDRANT_HOST,
            port=settings.DATABASE.QDRANT_PORT,
            timeout=10,
            check_compatibility=False
        )

        collections = client.get_collections().collections
        print(f'  集合总数: {len(collections)}')

        for col in collections:
            try:
                info = client.get_collection(col.name)
                count = info.points_count
                status = '[OK]' if count > 0 else '[EMPTY]'
                print(f'  {status} {col.name}: {count} 条向量')
            except Exception as e:
                print(f'  [ERROR] {col.name}: 错误 - {str(e)[:50]}')

        return True
    except Exception as e:
        print(f'  [ERROR] Qdrant 连接失败: {e}')
        return False


def check_nebula():
    """检查 NebulaGraph 数据"""
    print('\n' + '=' * 60)
    print('检查 NebulaGraph 数据')
    print('=' * 60)

    try:
        from database.nebula.connection import NebulaConnection

        NebulaConnection.init_connection()
        session = NebulaConnection.get_session()

        if not session:
            print('  [ERROR] NebulaGraph 获取会话失败')
            return False

        # 检查各种顶点数量
        tags = [
            'Institution',
            'Doctor',
            'Project',
            'Product',
            'Customer',
        ]

        for tag in tags:
            try:
                result = session.execute(f'MATCH (v:{tag}) RETURN count(v) as cnt')
                if result and result.is_succeeded():
                    count = result.row_values(0)[0].as_int() if result.row_size() > 0 else 0
                    status = '[OK]' if count > 0 else '[EMPTY]'
                    print(f'  {status} {tag}: {count} 个顶点')
                else:
                    print(f'  [EMPTY] {tag}: 查询无结果')
            except Exception as e:
                print(f'  [ERROR] {tag}: 错误 - {str(e)[:50]}')

        NebulaConnection.close_session()
        return True
    except Exception as e:
        print(f'  [ERROR] NebulaGraph 连接失败: {e}')
        return False


async def check_postgres():
    """检查 PostgreSQL 数据"""
    print('\n' + '=' * 60)
    print('检查 PostgreSQL 数据')
    print('=' * 60)

    try:
        from database.postgres.connection import PostgreSQLConnection

        pool = await PostgreSQLConnection.create_pool()

        tables = [
            ('机构', 'institution'),
            ('医生', 'doctor'),
            ('项目', 'project'),
            ('产品', 'product'),
            ('自然人', 'person'),
            ('机构客户', 'institution_customer'),
            ('消费订单', 'consumption_order'),
        ]

        async with pool.acquire() as conn:
            for name, table in tables:
                try:
                    result = await conn.fetchval(f'SELECT COUNT(*) FROM {table}')
                    status = '[OK]' if result > 0 else '[EMPTY]'
                    print(f'  {status} {name} ({table}): {result} 条')
                except Exception as e:
                    print(f'  [ERROR] {name} ({table}): 错误 - {str(e)[:50]}')

        await PostgreSQLConnection.close_pool()
        return True
    except Exception as e:
        print(f'  [ERROR] PostgreSQL 连接失败: {e}')
        return False


async def main():
    print('\n' + '=' * 60)
    print('  医美客户回访系统 - 数据同步检查')
    print('=' * 60)

    # 检查 PostgreSQL (主数据库)
    await check_postgres()

    # 检查 ClickHouse
    await check_clickhouse()

    # 检查 Qdrant
    check_qdrant()

    # 检查 NebulaGraph
    check_nebula()

    print('\n' + '=' * 60)
    print('检查完成')
    print('=' * 60)


if __name__ == '__main__':
    asyncio.run(main())

