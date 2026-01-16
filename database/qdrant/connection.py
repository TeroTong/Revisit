# database/qdrant/connection.py
import logging
import time
import os
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager
import requests

from qdrant_client import QdrantClient
from qdrant_client.http.exceptions import UnexpectedResponse

from config import settings

logger = logging.getLogger(__name__)


def _setup_no_proxy_for_localhost():
    """设置 NO_PROXY 环境变量，确保本地地址不走代理"""
    no_proxy_hosts = "localhost,127.0.0.1,::1"
    current_no_proxy = os.environ.get("NO_PROXY", os.environ.get("no_proxy", ""))

    if current_no_proxy:
        # 合并已有的 NO_PROXY 设置
        hosts_set = set(current_no_proxy.split(","))
        for host in no_proxy_hosts.split(","):
            hosts_set.add(host)
        new_no_proxy = ",".join(hosts_set)
    else:
        new_no_proxy = no_proxy_hosts

    os.environ["NO_PROXY"] = new_no_proxy
    os.environ["no_proxy"] = new_no_proxy


# 在模块加载时就设置 NO_PROXY
_setup_no_proxy_for_localhost()


# 统一获取Qdrant主机端口，兼容旧的顶层字段
def _get_qdrant_host_port():
    db_cfg = getattr(settings, "DATABASE", None)
    host = getattr(db_cfg, "QDRANT_HOST", getattr(settings, "QDRANT_HOST", "127.0.0.1"))
    port = getattr(db_cfg, "QDRANT_PORT", getattr(settings, "QDRANT_PORT", 6333))
    return host, port


class QdrantConnection:
    """Qdrant连接管理类"""

    _instance: Optional['QdrantConnection'] = None
    _client: Optional[QdrantClient] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.initialized = False
            self._client = None

    def get_client(self) -> QdrantClient:
        """获取Qdrant客户端实例"""
        if self._client is None:
            self._initialize_client()
        return self._client

    def _initialize_client(self):
        """初始化Qdrant客户端"""
        try:
            host, port = _get_qdrant_host_port()
            logger.info(f"正在连接Qdrant: {host}:{port}")

            # 使用配置中的主机和端口
            self._client = QdrantClient(
                host=host,
                port=port,
                timeout=30,
                prefer_grpc=False,
                https=False,
                check_compatibility=False,  # 跳过版本检查，避免额外请求
            )

            # 测试连接
            self._test_connection_with_retry()

            self.initialized = True
            logger.info("✅ Qdrant连接成功")

        except Exception as e:
            logger.error(f"❌ Qdrant连接失败: {e}")
            self._client = None
            raise

    def _test_connection_with_retry(self, max_retries: int = 3, retry_delay: int = 2):
        """带重试的连接测试"""
        for attempt in range(max_retries):
            try:
                collections = self._client.get_collections()
                logger.debug(f"当前集合数量: {len(collections.collections)}")
                return True
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"连接测试失败 (尝试 {attempt + 1}/{max_retries}): {e}")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"连接测试失败，已达到最大重试次数: {e}")
                    raise

    def close(self):
        """关闭连接"""
        if self._client:
            try:
                self._client = None
                self.initialized = False
                logger.info("Qdrant连接已关闭")
            except Exception as e:
                logger.error(f"关闭连接时出错: {e}")

    def health_check(self) -> Dict[str, Any]:
        """健康检查"""
        try:
            client = self.get_client()
            collections = client.get_collections()

            collection_status = {}
            for collection in collections.collections:
                try:
                    info = client.get_collection(collection.name)
                    collection_status[collection.name] = {
                        "status": "healthy",
                        "vectors_count": info.vectors_count,
                        "points_count": info.points_count,
                    }
                except Exception as e:
                    collection_status[collection.name] = {
                        "status": "unhealthy",
                        "error": str(e)
                    }

            return {
                "status": "healthy",
                "service": "qdrant",
                "version": "unknown",
                "collections": collection_status,
                "total_collections": len(collections.collections),
            }
        except Exception as e:
            logger.error(f"健康检查失败: {e}")
            return {
                "status": "unhealthy",
                "service": "qdrant",
                "error": str(e)
            }

    @asynccontextmanager
    async def get_async_client(self):
        """获取异步上下文管理的客户端"""
        client = self.get_client()
        try:
            yield client
        finally:
            pass

    def recreate_client(self):
        """重新创建客户端"""
        logger.info("正在重新创建Qdrant客户端...")
        self.close()
        self._initialize_client()

    def execute_query_with_retry(self, query_func, *args, **kwargs):
        """执行查询并处理连接异常，带重试"""
        max_retries = 3
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                client = self.get_client()
                return query_func(client, *args, **kwargs)
            except (UnexpectedResponse, ConnectionError) as e:
                if attempt < max_retries - 1:
                    logger.warning(f"查询失败，尝试重连 (尝试 {attempt + 1}/{max_retries}): {e}")
                    self.recreate_client()
                    time.sleep(retry_delay)
                else:
                    logger.error(f"查询执行失败，已达到最大重试次数: {e}")
                    raise
            except Exception as e:
                logger.error(f"查询执行失败: {e}")
                raise


# 全局连接实例
qdrant_connection = QdrantConnection()


# 便捷函数
def get_qdrant_client() -> QdrantClient:
    """获取Qdrant客户端"""
    return qdrant_connection.get_client()


async def test_qdrant_connection() -> bool:
    """测试Qdrant连接（异步版本）"""
    try:
        client = get_qdrant_client()
        client.get_collections()
        return True
    except Exception as e:
        logger.error(f"Qdrant连接测试失败: {e}")
        return False


def test_qdrant_connection_simple() -> bool:
    """测试Qdrant连接（简单同步版本）"""
    try:
        client = get_qdrant_client()
        client.get_collections()
        logger.info("✅ Qdrant连接测试成功")
        return True
    except Exception as e:
        logger.error(f"❌ Qdrant连接测试失败: {e}")
        return False


def close_qdrant_connection():
    """关闭Qdrant连接"""
    qdrant_connection.close()


def get_qdrant_health() -> Dict[str, Any]:
    """获取Qdrant健康状态"""
    return qdrant_connection.health_check()


# 兼容旧代码的函数
def init_qdrant_connection() -> bool:
    """初始化Qdrant连接（用于旧代码兼容）"""
    return test_qdrant_connection_simple()


# 异步健康检查
async def async_health_check() -> Dict[str, Any]:
    """异步健康检查"""
    try:
        # 使用异步上下文管理器
        async with qdrant_connection.get_async_client() as client:
            collections = client.get_collections()

            collection_status = {}
            for collection in collections.collections:
                try:
                    info = client.get_collection(collection.name)
                    collection_status[collection.name] = {
                        "status": "healthy",
                        "vectors_count": info.vectors_count,
                        "points_count": info.points_count,
                    }
                except Exception as e:
                    collection_status[collection.name] = {
                        "status": "unhealthy",
                        "error": str(e)
                    }

            return {
                "status": "healthy",
                "service": "qdrant",
                "version": "unknown",
                "collections": collection_status,
                "total_collections": len(collections.collections),
            }
    except Exception as e:
        logger.error(f"异步健康检查失败: {e}")
        return {
            "status": "unhealthy",
            "service": "qdrant",
            "error": str(e)
        }


# 等待Qdrant服务就绪
def wait_for_qdrant(timeout: int = 60, check_interval: int = 5, http_check: bool = True) -> bool:
    """等待Qdrant服务就绪，优先使用HTTP /collections 探测，失败后回退到 QdrantClient。

    保持同步返回 bool 以兼容现有调用。
    """
    logger.info(f"等待Qdrant服务就绪，超时时间: {timeout}秒, 检查间隔: {check_interval}s, http_check: {http_check}")

    start_time = time.time()
    attempts = 0

    host, port = _get_qdrant_host_port()

    def http_probe():
        try:
            url = f"http://{host}:{port}/collections"
            resp = requests.get(url, timeout=min(5, max(1, check_interval)))
            logger.debug(f"HTTP probe {url} -> status {resp.status_code}")
            if resp.status_code == 200:
                # optional basic validation of returned JSON
                try:
                    j = resp.json()
                    if isinstance(j, dict) and 'collections' in j:
                        return True
                except Exception:
                    # still consider 200 as success in many qdrant versions
                    return True
            return False
        except Exception as e:
            logger.debug(f"HTTP probe exception: {e}")
            return False

    def client_probe(prefer_grpc_flag: bool):
        try:
            client = QdrantClient(host=host, port=port, timeout=min(10, max(3, check_interval)), prefer_grpc=prefer_grpc_flag)
            # lightweight call
            cols = client.get_collections()
            logger.debug(f"QdrantClient probe(prefer_grpc={prefer_grpc_flag}) -> collections: {len(getattr(cols, 'collections', []))}")
            return True
        except Exception as e:
            logger.debug(f"QdrantClient probe(prefer_grpc={prefer_grpc_flag}) exception: {e}")
            return False

    while time.time() - start_time < timeout:
        attempts += 1
        try:
            if http_check and http_probe():
                logger.info(f"✅ Qdrant 服务在第 {attempts} 次尝试后通过 HTTP 探测准备就绪")
                return True

            # try client probes: first HTTP transport (prefer_grpc=False), then gRPC
            if client_probe(prefer_grpc_flag=False) or client_probe(prefer_grpc_flag=True):
                logger.info(f"✅ Qdrant 服务在第 {attempts} 次尝试后通过 QdrantClient 准备就绪")
                return True

            logger.debug(f"第 {attempts} 次尝试未就绪，等待 {check_interval}s 后重试")
            time.sleep(check_interval)
        except Exception as e:
            logger.debug(f"第 {attempts} 次检查异常: {e}")
            time.sleep(check_interval)

    logger.error(f"❌ Qdrant服务在 {timeout} 秒后仍未就绪（尝试次数: {attempts}）")
    return False

