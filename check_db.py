"""检查数据库表创建状态"""
import os

# 禁用代理设置（解决远程数据库连接问题）
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    os.environ.pop(key, None)

import asyncio

async def check_postgresql():
    """检查 PostgreSQL 表"""
    print("\n" + "=" * 50)
    print("PostgreSQL 表检查")
    print("=" * 50)

    try:
        from database.postgres.connection import create_pool, close_pool
        pool = await create_pool()
        async with pool.acquire() as conn:
            tables = await conn.fetch(
                "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename"
            )
            print(f"✅ PostgreSQL: {len(tables)} 张表")
            for t in tables:
                print(f"   - {t['tablename']}")
        await close_pool()
    except Exception as e:
        print(f"❌ PostgreSQL 检查失败: {e}")


async def check_clickhouse():
    """检查 ClickHouse 表"""
    print("\n" + "=" * 50)
    print("ClickHouse 表检查")
    print("=" * 50)

    try:
        from database.clickhouse.connection import get_clickhouse_client, close_clickhouse_client
        client = await get_clickhouse_client()
        tables = await client.fetch("SHOW TABLES FROM revisit")
        print(f"✅ ClickHouse: {len(tables)} 张表")
        for t in tables:
            print(f"   - {t[0]}")
        await close_clickhouse_client()
    except Exception as e:
        print(f"❌ ClickHouse 检查失败: {e}")


def check_nebula():
    """检查 NebulaGraph Schema"""
    print("\n" + "=" * 50)
    print("NebulaGraph Schema 检查")
    print("=" * 50)

    try:
        from database.nebula.connection import NebulaConnection
        conn = NebulaConnection()
        session = conn.get_session()

        # 检查 Tags
        result = session.execute("USE revisit; SHOW TAGS;")
        if result.is_succeeded():
            tags = result.column_values("Name")
            print(f"✅ Tags: {len(tags)} 个")
            for tag in tags:
                print(f"   - {tag.as_string()}")

        # 检查 Edges
        result = session.execute("SHOW EDGES;")
        if result.is_succeeded():
            edges = result.column_values("Name")
            print(f"✅ Edges: {len(edges)} 个")
            for edge in edges:
                print(f"   - {edge.as_string()}")

        conn.close()
    except Exception as e:
        print(f"❌ NebulaGraph 检查失败: {e}")


def check_qdrant():
    """检查 Qdrant 集合"""
    print("\n" + "=" * 50)
    print("Qdrant 集合检查")
    print("=" * 50)

    try:
        from database.qdrant.connection import get_qdrant_client
        client = get_qdrant_client()
        collections = client.get_collections()
        print(f"✅ Qdrant: {len(collections.collections)} 个集合")
        for col in collections.collections:
            print(f"   - {col.name}")
    except Exception as e:
        print(f"❌ Qdrant 检查失败: {e}")


async def main():
    print("\n" + "=" * 50)
    print("数据库表创建状态检查")
    print("=" * 50)

    await check_postgresql()
    await check_clickhouse()
    check_nebula()
    check_qdrant()

    print("\n" + "=" * 50)
    print("检查完成")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())

