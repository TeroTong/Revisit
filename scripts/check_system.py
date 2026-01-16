"""
系统检查脚本

用于检查系统各组件的状态和配置
用法：python -m scripts.check_system
"""

import os

# 禁用代理设置（解决远程数据库连接问题）
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    os.environ.pop(key, None)

import asyncio
import sys
from pathlib import Path

# 添加项目根目录
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import settings


async def check_postgresql():
    """检查 PostgreSQL 连接"""
    conn = None
    try:
        import asyncpg
        # 直接创建临时连接测试，不使用连接池
        conn = await asyncio.wait_for(
            asyncpg.connect(
                host=settings.DATABASE.POSTGRES_HOST,
                port=settings.DATABASE.POSTGRES_PORT,
                user=settings.DATABASE.POSTGRES_USER,
                password=settings.DATABASE.POSTGRES_PASSWORD,
                database=settings.DATABASE.POSTGRES_DB,
            ),
            timeout=10
        )
        result = await conn.fetchval("SELECT version()")
        return True, result.split(',')[0] if result else "Connected"
    except asyncio.TimeoutError:
        return False, "Connection timeout"
    except Exception as e:
        return False, str(e)[:100]
    finally:
        if conn:
            await conn.close()


async def check_nebulagraph():
    """检查 NebulaGraph 连接"""
    try:
        from nebula3.gclient.net import ConnectionPool
        from nebula3.Config import Config

        config = Config()
        config.max_connection_pool_size = 1
        config.timeout = 5000

        pool = ConnectionPool()
        ok = pool.init(
            [(settings.DATABASE.NEBULA_HOST, settings.DATABASE.NEBULA_PORT)],
            config
        )

        if not ok:
            return False, "Pool init failed"

        session = pool.get_session(
            settings.DATABASE.NEBULA_USER,
            settings.DATABASE.NEBULA_PASSWORD
        )

        try:
            result = session.execute("SHOW SPACES")
            if result.is_succeeded():
                return True, f"Space: {settings.DATABASE.NEBULA_SPACE}"
            return False, f"Query failed: {result.error_msg()}"
        finally:
            session.release()
            pool.close()

    except Exception as e:
        return False, str(e)[:100]


async def check_qdrant():
    """检查 Qdrant 连接"""
    try:
        import aiohttp
        import os

        # 设置不走代理
        no_proxy = os.environ.get("NO_PROXY", "")
        if "localhost" not in no_proxy:
            os.environ["NO_PROXY"] = f"{no_proxy},localhost,127.0.0.1"

        url = f"http://{settings.DATABASE.QDRANT_HOST}:{settings.DATABASE.QDRANT_PORT}/collections"

        # 创建不使用代理的 connector
        connector = aiohttp.TCPConnector(force_close=True)
        async with aiohttp.ClientSession(connector=connector, trust_env=False) as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    collections = data.get("result", {}).get("collections", [])
                    return True, f"{len(collections)} collections"
                return False, f"HTTP {resp.status}"
    except asyncio.TimeoutError:
        return False, "Connection timeout"
    except Exception as e:
        return False, str(e)[:100]


async def check_clickhouse():
    """检查 ClickHouse 连接"""
    try:
        import aiohttp
        url = f"http://{settings.DATABASE.CLICKHOUSE_HOST}:{settings.DATABASE.CLICKHOUSE_PORT}"

        async with aiohttp.ClientSession() as session:
            async with session.get(f"{url}/ping", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    return True, f"Database: {settings.DATABASE.CLICKHOUSE_DB}"
                return False, f"HTTP {resp.status}"
    except asyncio.TimeoutError:
        return False, "Connection timeout"
    except Exception as e:
        return False, str(e)[:100]


async def check_llm():
    """检查 LLM 服务"""
    try:
        from services.llm_service import LLMService
        service = LLMService()
        if not service.enabled:
            return False, "API Key not configured"

        # 简单测试
        success = await service.test_connection()
        if success:
            return True, f"Model: {settings.LLM.OPENAI_MODEL}"
        return False, "Connection test failed"
    except Exception as e:
        return False, str(e)[:100]


def check_data_files():
    """检查数据文件"""
    data_dir = Path(settings.APP.DATA_DIR)
    import_dir = data_dir / "import"

    results = []

    # 检查初始数据
    initial_dir = import_dir / "initial"
    if initial_dir.exists():
        common_files = list((initial_dir / "common").glob("*.json"))
        results.append(f"Common files: {len(common_files)}")

        institutions_dir = initial_dir / "institutions"
        if institutions_dir.exists():
            inst_dirs = [d for d in institutions_dir.iterdir() if d.is_dir()]
            results.append(f"Institutions: {len(inst_dirs)}")
    else:
        results.append("Initial dir not found")

    # 检查增量数据
    pending_dir = import_dir / "incremental" / "pending"
    if pending_dir.exists():
        pending = [d for d in pending_dir.iterdir() if d.is_dir()]
        results.append(f"Pending batches: {len(pending)}")

    return "; ".join(results)


async def main():
    """主函数"""
    print("\n" + "=" * 60)
    print("  医美客户回访系统 - 系统检查")
    print("=" * 60)

    print(f"\n配置信息:")
    print(f"  环境: {settings.APP.ENVIRONMENT}")
    print(f"  调试模式: {settings.APP.DEBUG}")
    print(f"  机构列表: {settings.APP.INSTITUTIONS}")
    print(f"  回访提前天数: {settings.APP.BIRTHDAY_REMINDER_DAYS_AHEAD}")

    print(f"\n数据库配置:")
    print(f"  PostgreSQL: {settings.DATABASE.POSTGRES_HOST}:{settings.DATABASE.POSTGRES_PORT}/{settings.DATABASE.POSTGRES_DB}")
    print(f"  NebulaGraph: {settings.DATABASE.NEBULA_HOST}:{settings.DATABASE.NEBULA_PORT}/{settings.DATABASE.NEBULA_SPACE}")
    print(f"  Qdrant: {settings.DATABASE.QDRANT_HOST}:{settings.DATABASE.QDRANT_PORT}")
    print(f"  ClickHouse: {settings.DATABASE.CLICKHOUSE_HOST}:{settings.DATABASE.CLICKHOUSE_PORT}/{settings.DATABASE.CLICKHOUSE_DB}")

    print(f"\nLLM配置:")
    print(f"  Model: {settings.LLM.OPENAI_MODEL}")
    print(f"  API URL: {settings.LLM.OPENAI_API_BASE}")

    print("\n" + "-" * 60)
    print("连接检查:")
    print("-" * 60)

    checks = [
        ("PostgreSQL", check_postgresql()),
        ("NebulaGraph", check_nebulagraph()),
        ("Qdrant", check_qdrant()),
        ("ClickHouse", check_clickhouse()),
        ("LLM Service", check_llm()),
    ]

    all_ok = True
    for name, check_coro in checks:
        success, info = await check_coro
        status = "✅" if success else "❌"
        if not success:
            all_ok = False
        print(f"  {status} {name}: {info[:50]}..." if len(str(info)) > 50 else f"  {status} {name}: {info}")

    print("\n" + "-" * 60)
    print("数据文件:")
    print("-" * 60)
    data_info = check_data_files()
    print(f"  {data_info}")

    print("\n" + "=" * 60)
    if all_ok:
        print("  ✅ 系统检查通过")
    else:
        print("  ⚠️ 部分组件不可用，请检查配置")
    print("=" * 60 + "\n")

    return 0 if all_ok else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

