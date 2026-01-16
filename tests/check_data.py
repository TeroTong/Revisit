"""简单的数据库数据检查脚本"""
import os
import sys

# 设置输出编码
sys.stdout.reconfigure(encoding='utf-8')

# 禁用代理
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    os.environ.pop(key, None)

import asyncio


async def check_postgres():
    """检查 PostgreSQL 数据"""
    print('=' * 60)
    print('PostgreSQL 数据检查')
    print('=' * 60)

    from database.postgres.connection import PostgreSQLConnection
    pool = await PostgreSQLConnection.create_pool()

    async with pool.acquire() as conn:
        # 列出所有表
        tables = await conn.fetch(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
        )
        print(f'\n共有 {len(tables)} 张表:')
        for row in tables:
            table_name = row['tablename']
            count = await conn.fetchval(f'SELECT COUNT(*) FROM "{table_name}"')
            status = '[OK]' if count > 0 else '[EMPTY]'
            print(f'  {status} {table_name}: {count} 条')

    await PostgreSQLConnection.close_pool()


async def check_clickhouse():
    """检查 ClickHouse 数据"""
    print('\n' + '=' * 60)
    print('ClickHouse 数据检查')
    print('=' * 60)

    try:
        from database.clickhouse.connection import ClickHouseConnection
        client = await ClickHouseConnection.get_client()

        # 获取所有表
        tables_result = await client.fetch("SHOW TABLES")
        print(f'\n共有 {len(tables_result)} 张表:')

        for row in tables_result:
            table_name = row['name']
            count_result = await client.fetch(f'SELECT COUNT(*) as cnt FROM {table_name}')
            count = count_result[0]['cnt'] if count_result else 0
            status = '[OK]' if count > 0 else '[EMPTY]'
            print(f'  {status} {table_name}: {count} 条')

        await ClickHouseConnection.close_client()
    except Exception as e:
        print(f'  [ERROR] ClickHouse 连接失败: {e}')


def check_qdrant():
    """检查 Qdrant 数据"""
    print('\n' + '=' * 60)
    print('Qdrant 数据检查')
    print('=' * 60)

    # 再次确保禁用代理
    import os
    for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy', 'NO_PROXY', 'no_proxy']:
        os.environ.pop(key, None)
    os.environ['NO_PROXY'] = '*'
    os.environ['no_proxy'] = '*'

    try:
        from qdrant_client import QdrantClient
        from config.settings import settings
        import httpx

        # 使用不带代理的 httpx client
        http_client = httpx.Client(proxy=None)

        client = QdrantClient(
            host=settings.DATABASE.QDRANT_HOST,
            port=settings.DATABASE.QDRANT_PORT,
            timeout=10,
            check_compatibility=False,
            prefer_grpc=False  # 使用 HTTP 而不是 gRPC
        )

        collections = client.get_collections().collections
        print(f'\n共有 {len(collections)} 个集合:')

        for col in collections:
            info = client.get_collection(col.name)
            count = info.points_count
            status = '[OK]' if count > 0 else '[EMPTY]'
            print(f'  {status} {col.name}: {count} 条向量')

    except Exception as e:
        print(f'  [ERROR] Qdrant 连接失败: {e}')
        print(f'  提示: 请检查代理设置，或尝试手动访问 http://localhost:6333/collections')


def check_nebula():
    """检查 NebulaGraph 数据"""
    print('\n' + '=' * 60)
    print('NebulaGraph 数据检查')
    print('=' * 60)

    try:
        from database.nebula.connection import NebulaConnection
        from config.settings import settings

        NebulaConnection.init_connection()
        session = NebulaConnection.get_session()

        if not session:
            print('  [ERROR] 获取会话失败')
            return

        # 先选择 space
        space_name = settings.DATABASE.NEBULA_SPACE
        use_result = session.execute(f'USE {space_name}')
        if not use_result.is_succeeded():
            print(f'  [ERROR] 无法选择 space {space_name}: {use_result.error_msg()}')
            return

        tags = ['institution', 'doctor', 'project', 'product', 'institution_customer']
        print(f'\n检查 {len(tags)} 种顶点:')

        for tag in tags:
            result = session.execute(f'MATCH (v:{tag}) RETURN count(v) as cnt')
            if result and result.is_succeeded() and result.row_size() > 0:
                count = result.row_values(0)[0].as_int()
                status = '[OK]' if count > 0 else '[EMPTY]'
                print(f'  {status} {tag}: {count} 个顶点')
            else:
                err_msg = result.error_msg() if result else 'No result'
                print(f'  [EMPTY] {tag}: 0 (error: {err_msg})')

        NebulaConnection.close_session()

    except Exception as e:
        print(f'  [ERROR] NebulaGraph 连接失败: {e}')


async def main():
    print('\n' + '=' * 60)
    print('  医美客户回访系统 - 数据同步检查')
    print('=' * 60 + '\n')

    await check_postgres()
    await check_clickhouse()
    check_qdrant()
    check_nebula()

    print('\n' + '=' * 60)
    print('检查完成')
    print('=' * 60)


if __name__ == '__main__':
    asyncio.run(main())

