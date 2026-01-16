"""修复数据同步脚本
用于将 PostgreSQL 中已存在但未同步的数据同步到其他数据库
"""
import os
import sys

# 设置输出编码
sys.stdout.reconfigure(encoding='utf-8')

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 禁用代理
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    os.environ.pop(key, None)

import asyncio
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def fix_clickhouse_customers():
    """修复 ClickHouse 客户数据"""
    print('=' * 60)
    print('修复 ClickHouse 客户数据')
    print('=' * 60)

    from services.data_sync import DataSyncService

    sync_service = DataSyncService()
    await sync_service.init_connections()

    try:
        count = await sync_service.sync_existing_customers_to_clickhouse()
        print(f'\n✅ 同步完成，共同步 {count} 条客户记录')
    except Exception as e:
        print(f'\n❌ 同步失败: {e}')
    finally:
        await sync_service.close_connections()


async def fix_nebula_data():
    """修复 NebulaGraph 数据"""
    print('\n' + '=' * 60)
    print('修复 NebulaGraph 数据')
    print('=' * 60)

    from database.postgres.connection import PostgreSQLConnection
    from database.nebula.connection import NebulaConnection, execute_ngql
    from config.settings import settings

    await PostgreSQLConnection.create_pool()
    NebulaConnection.init_connection()
    session = NebulaConnection.get_session()

    if not session:
        print('❌ 无法获取 NebulaGraph 会话')
        return

    # 先选择 space
    execute_ngql(f'USE {settings.DATABASE.NEBULA_SPACE}')

    synced_count = 0

    try:
        pool = await PostgreSQLConnection.create_pool()

        async with pool.acquire() as conn:
            # 同步机构
            institutions = await conn.fetch('SELECT institution_id, institution_code, name, type, status FROM institution')
            for inst in institutions:
                vid = f"inst_{inst['institution_code']}"
                query = f'''
                    INSERT VERTEX IF NOT EXISTS institution(
                        institution_id, institution_code, name, alias, type, status
                    ) VALUES "{vid}":(
                        "{inst['institution_id']}", "{inst['institution_code']}", 
                        "{inst['name']}", "", "{inst['type'] or ''}", "{inst['status']}"
                    )
                '''
                result = execute_ngql(query)
                if result and result.is_succeeded():
                    synced_count += 1
            print(f'  ✅ 同步机构: {len(institutions)} 条')

            # 同步医生
            doctors = await conn.fetch('SELECT doctor_id, doctor_code, name, gender, phone, institution_code, title FROM doctor')
            for doc in doctors:
                vid = f"doc_{doc['doctor_code']}"
                query = f'''
                    INSERT VERTEX IF NOT EXISTS doctor(
                        doctor_id, doctor_code, name, gender, phone, institution_code, title
                    ) VALUES "{vid}":(
                        "{doc['doctor_id']}", "{doc['doctor_code']}", 
                        "{doc['name']}", "{doc['gender'] or ''}", "{doc['phone'] or ''}",
                        "{doc['institution_code'] or ''}", "{doc['title'] or ''}"
                    )
                '''
                result = execute_ngql(query)
                if result and result.is_succeeded():
                    synced_count += 1
            print(f'  ✅ 同步医生: {len(doctors)} 条')

            # 同步项目
            projects = await conn.fetch('SELECT project_id, project_code, name, category, body_part, risk_level FROM project')
            for proj in projects:
                vid = f"proj_{proj['project_code']}"
                query = f'''
                    INSERT VERTEX IF NOT EXISTS project(
                        project_id, project_code, name, category, body_part, risk_level
                    ) VALUES "{vid}":(
                        "{proj['project_id']}", "{proj['project_code']}", 
                        "{proj['name']}", "{proj['category'] or ''}", 
                        "{proj['body_part'] or ''}", {proj['risk_level'] or 1}
                    )
                '''
                result = execute_ngql(query)
                if result and result.is_succeeded():
                    synced_count += 1
            print(f'  ✅ 同步项目: {len(projects)} 条')

            # 同步产品
            products = await conn.fetch('SELECT product_id, product_code, name, brand, category, body_part FROM product')
            for prod in products:
                vid = f"prod_{prod['product_code']}"
                query = f'''
                    INSERT VERTEX IF NOT EXISTS product(
                        product_id, product_code, name, brand, category, body_part
                    ) VALUES "{vid}":(
                        "{prod['product_id']}", "{prod['product_code']}", 
                        "{prod['name']}", "{prod['brand'] or ''}", 
                        "{prod['category'] or ''}", "{prod['body_part'] or ''}"
                    )
                '''
                result = execute_ngql(query)
                if result and result.is_succeeded():
                    synced_count += 1
            print(f'  ✅ 同步产品: {len(products)} 条')

            # 同步客户 (遍历所有机构)
            customer_count = 0
            inst_codes = await conn.fetch('SELECT institution_code FROM institution')
            for inst in inst_codes:
                institution_code = inst['institution_code']
                suffix = institution_code.lower().replace('-', '_')
                customer_table = f"institution_customer_{suffix}"

                # 检查表是否存在
                exists = await conn.fetchval('''
                    SELECT EXISTS (
                        SELECT FROM pg_tables 
                        WHERE schemaname = 'public' 
                        AND tablename = $1
                    )
                ''', customer_table)

                if not exists:
                    continue

                customers = await conn.fetch(f'''
                    SELECT 
                        ic.institution_customer_id::text, ic.customer_code, ic.vip_level, ic.status,
                        np.name, np.phone, np.gender, np.birthday
                    FROM {customer_table} ic
                    JOIN natural_person np ON ic.person_id = np.person_id
                ''')

                for cust in customers:
                    vid = f"cust_{cust['customer_code']}"
                    birthday_str = cust['birthday'].strftime('%Y-%m-%d') if cust['birthday'] else ''
                    query = f'''
                        INSERT VERTEX IF NOT EXISTS institution_customer(
                            institution_customer_id, customer_code, name, phone, gender, birthday
                        ) VALUES "{vid}":(
                            "{cust['institution_customer_id']}", "{cust['customer_code']}", 
                            "{cust['name']}", "{cust['phone'] or ''}", 
                            "{cust['gender'] or ''}", "{birthday_str}"
                        )
                    '''
                    result = execute_ngql(query)
                    if result and result.is_succeeded():
                        customer_count += 1
                        synced_count += 1

            print(f'  ✅ 同步客户: {customer_count} 条')

        print(f'\n✅ NebulaGraph 同步完成，共同步 {synced_count} 条记录')

    except Exception as e:
        print(f'\n❌ 同步失败: {e}')
        import traceback
        traceback.print_exc()
    finally:
        NebulaConnection.close_session()
        await PostgreSQLConnection.close_pool()


async def check_qdrant():
    """检查 Qdrant 状态并清理多余集合"""
    print('\n' + '=' * 60)
    print('检查 Qdrant 数据库')
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
        print(f'\n  集合总数: {len(collections)}')

        # 获取配置的集合列表
        configured = set(settings.DATABASE.QDRANT_COLLECTIONS)
        existing = {c.name for c in collections}
        extra = existing - configured

        # 显示所有集合状态
        for col in collections:
            info = client.get_collection(col.name)
            count = info.points_count
            is_extra = col.name in extra
            if is_extra:
                status = '⚠️ [多余]'
            elif count > 0:
                status = '✅'
            else:
                status = '⚠️ [空]'
            print(f'  {status} {col.name}: {count} 条向量')

        # 清理多余的集合
        if extra:
            print(f'\n  正在清理多余的集合: {extra}')
            for name in extra:
                try:
                    client.delete_collection(name)
                    print(f'    ✅ 已删除: {name}')
                except Exception as e:
                    print(f'    ❌ 删除失败 {name}: {e}')

            # 显示清理后的状态
            print(f'\n  清理后集合数: {len(client.get_collections().collections)}')

        print('\n✅ Qdrant 检查完成')
    except Exception as e:
        print(f'\n❌ Qdrant 连接失败: {e}')
        print('   请检查 Qdrant 服务是否启动，或是否存在代理问题')


async def main():
    print('\n' + '=' * 60)
    print('  医美客户回访系统 - 数据同步修复')
    print('=' * 60 + '\n')

    # 修复 ClickHouse 客户数据
    await fix_clickhouse_customers()

    # 修复 NebulaGraph 数据
    await fix_nebula_data()

    # 检查 Qdrant 状态
    await check_qdrant()

    print('\n' + '=' * 60)
    print('修复完成')
    print('=' * 60)


if __name__ == '__main__':
    asyncio.run(main())

