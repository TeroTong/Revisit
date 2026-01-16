# database/qdrant/schema.py
import logging
import time
from typing import Dict, Any, Optional, List
from datetime import datetime

from qdrant_client import QdrantClient
from qdrant_client.http.models import (
    VectorParams, Distance, CollectionStatus,
    HnswConfigDiff, OptimizersConfigDiff, KeywordIndexParams
)

from database.qdrant.connection import wait_for_qdrant, _get_qdrant_host_port
from config.settings import settings

logger = logging.getLogger(__name__)


def get_configured_collections() -> List[str]:
    """从配置中获取要创建的集合列表"""
    return settings.DATABASE.QDRANT_COLLECTIONS


class QdrantSchemaManager:
    """Qdrant集合Schema管理器"""

    # 向量配置
    VECTOR_CONFIGS = {
        "customer_profiles": {"size": 1536, "distance": Distance.COSINE},
        "medical_knowledge": {"size": 1536, "distance": Distance.COSINE},
        "consultation_patterns": {"size": 1536, "distance": Distance.COSINE}
    }
    DEFAULT_VECTOR_CONFIG = {"size": 1536, "distance": Distance.COSINE}

    # 集合配置
    COLLECTION_CONFIGS = {
        "customer_profiles": {
            "hnsw_config": HnswConfigDiff(m=16, ef_construct=100, full_scan_threshold=10000, max_indexing_threads=4),
            "optimizers_config": OptimizersConfigDiff(deleted_threshold=0.2, vacuum_min_vector_number=1000, max_segment_size=50000, memmap_threshold=20000, indexing_threshold=20000, flush_interval_sec=5, max_optimization_threads=4),
            "on_disk_payload": True
        },
        "medical_knowledge": {
            "hnsw_config": HnswConfigDiff(m=16, ef_construct=200, full_scan_threshold=5000, max_indexing_threads=2),
            "optimizers_config": OptimizersConfigDiff(deleted_threshold=0.1, vacuum_min_vector_number=500, max_segment_size=30000, memmap_threshold=15000, indexing_threshold=15000, flush_interval_sec=10, max_optimization_threads=2),
            "on_disk_payload": True
        },
        "consultation_patterns": {
            "hnsw_config": HnswConfigDiff(m=8, ef_construct=50, full_scan_threshold=2000, max_indexing_threads=2),
            "optimizers_config": OptimizersConfigDiff(deleted_threshold=0.05, vacuum_min_vector_number=200, max_segment_size=10000, memmap_threshold=5000, indexing_threshold=5000, flush_interval_sec=5, max_optimization_threads=2),
            "on_disk_payload": True
        }
    }
    DEFAULT_COLLECTION_CONFIG = {
        "hnsw_config": HnswConfigDiff(m=16, ef_construct=100, full_scan_threshold=10000, max_indexing_threads=2),
        "optimizers_config": OptimizersConfigDiff(deleted_threshold=0.2, vacuum_min_vector_number=1000, max_segment_size=50000, memmap_threshold=20000, indexing_threshold=20000, flush_interval_sec=5, max_optimization_threads=2),
        "on_disk_payload": True
    }

    # Payload字段索引配置
    PAYLOAD_INDEX_CONFIGS = {
        "customer_profiles": [
            {"field_name": "institution_code", "field_schema": "keyword"},
            {"field_name": "customer_id", "field_schema": "keyword"},
            {"field_name": "customer_code", "field_schema": "keyword"},
            {"field_name": "name", "field_schema": "keyword"},
            {"field_name": "phone", "field_schema": "keyword"},
            {"field_name": "gender", "field_schema": "keyword"},
            {"field_name": "vip_level", "field_schema": "keyword"},
            {"field_name": "status", "field_schema": "keyword"},
            {"field_name": "tags", "field_schema": "keyword"},
            {"field_name": "total_consumption", "field_schema": "float"},
            {"field_name": "created_at", "field_schema": "datetime"},
        ],
        "medical_knowledge": [
            {"field_name": "entity_type", "field_schema": "keyword"},
            {"field_name": "entity_id", "field_schema": "keyword"},
            {"field_name": "institution_code", "field_schema": "keyword"},
            {"field_name": "category", "field_schema": "keyword"},
            {"field_name": "tags", "field_schema": "keyword"},
            {"field_name": "name", "field_schema": "text"},
            {"field_name": "created_at", "field_schema": "datetime"},
        ],
        "consultation_patterns": [
            {"field_name": "institution_code", "field_schema": "keyword"},
            {"field_name": "customer_id", "field_schema": "keyword"},
            {"field_name": "intent", "field_schema": "keyword"},
            {"field_name": "channel", "field_schema": "keyword"},
            {"field_name": "status", "field_schema": "keyword"},
            {"field_name": "tags", "field_schema": "keyword"},
            {"field_name": "created_at", "field_schema": "datetime"},
        ]
    }

    def __init__(self, client: QdrantClient):
        self.client = client

    def get_vector_config(self, collection_name: str) -> Dict[str, Any]:
        return self.VECTOR_CONFIGS.get(collection_name, self.DEFAULT_VECTOR_CONFIG)

    def get_collection_config(self, collection_name: str) -> Dict[str, Any]:
        return self.COLLECTION_CONFIGS.get(collection_name, self.DEFAULT_COLLECTION_CONFIG)

    def get_payload_index_config(self, collection_name: str) -> List[Dict[str, Any]]:
        return self.PAYLOAD_INDEX_CONFIGS.get(collection_name, [])

    def initialize_all_collections(self) -> Dict[str, bool]:
        """初始化配置中指定的所有集合"""
        configured_collections = get_configured_collections()
        logger.info(f"配置的集合列表: {configured_collections}")

        results = {}
        for name in configured_collections:
            try:
                results[name] = self._create_collection(name)
            except Exception as e:
                logger.error(f"集合初始化异常 {name}: {e}")
                results[name] = False

        logger.info(f"集合初始化: {sum(results.values())}成功/{len(results)}总数")
        return results

    def _create_collection(self, collection_name: str) -> bool:
        """创建集合"""
        try:
            if self.collection_exists(collection_name):
                logger.info(f"集合已存在: {collection_name}")
                return True

            vector_config = self.get_vector_config(collection_name)
            collection_config = self.get_collection_config(collection_name)

            self.client.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(size=vector_config["size"], distance=vector_config["distance"]),
                hnsw_config=collection_config["hnsw_config"],
                optimizers_config=collection_config["optimizers_config"],
                on_disk_payload=collection_config.get("on_disk_payload", True),
            )
            time.sleep(0.5)
            logger.info(f"✅ 集合创建成功: {collection_name}")
            return True
        except Exception as e:
            if "already exists" in str(e).lower():
                return True
            logger.error(f"集合创建失败 {collection_name}: {e}")
            return False

    def collection_exists(self, collection_name: str) -> bool:
        try:
            collections = self.client.get_collections()
            return any(c.name == collection_name for c in collections.collections)
        except Exception as e:
            logger.error(f"检查集合存在性失败: {e}")
            return False

    def get_collection_info(self, collection_name: str) -> Optional[Dict[str, Any]]:
        try:
            info = self.client.get_collection(collection_name)
            return {
                "name": collection_name,
                "status": info.status.value if info.status else "UNKNOWN",
                "vectors_count": getattr(info, "vectors_count", None),
                "points_count": getattr(info, "points_count", None),
            }
        except Exception as e:
            logger.error(f"获取集合信息失败 {collection_name}: {e}")
            return None

    def get_all_collections_info(self) -> Dict[str, Any]:
        try:
            collections = self.client.get_collections()
            collections_info = {}
            for c in collections.collections:
                info = self.get_collection_info(c.name)
                collections_info[c.name] = info if info else {"name": c.name, "error": "获取失败"}
            return {"total_collections": len(collections.collections), "collections": collections_info}
        except Exception as e:
            return {"error": str(e)}

    def cleanup_unknown_collections(self, dry_run: bool = False) -> Dict[str, Any]:
        """清理不在配置中的集合"""
        result = {"deleted": [], "skipped": [], "errors": []}
        try:
            existing = {c.name for c in self.client.get_collections().collections}
            expected = set(get_configured_collections())
            extra = existing - expected

            if not extra:
                return result

            logger.info(f"发现多余集合: {list(extra)}")
            if dry_run:
                result["skipped"] = list(extra)
                return result

            for name in extra:
                try:
                    self.client.delete_collection(name)
                    result["deleted"].append(name)
                    logger.info(f"已删除: {name}")
                except Exception as e:
                    result["errors"].append({"name": name, "error": str(e)})
        except Exception as e:
            result["errors"].append({"error": str(e)})
        return result

    def health_check_all(self) -> Dict[str, Any]:
        results = {}
        for name in get_configured_collections():
            if self.collection_exists(name):
                try:
                    info = self.client.get_collection(name)
                    results[name] = {
                        "status": "healthy" if info.status == CollectionStatus.GREEN else "degraded",
                        "is_healthy": info.status == CollectionStatus.GREEN
                    }
                except Exception as e:
                    results[name] = {"status": "error", "error": str(e), "is_healthy": False}
            else:
                results[name] = {"status": "not_exists", "is_healthy": False}
        return {"collections": results, "timestamp": datetime.now().isoformat()}


# 便捷函数
def init_qdrant_schema(cleanup_extra: bool = False) -> bool:
    """初始化Qdrant Schema"""
    try:
        wait_for_qdrant(timeout=30, check_interval=3, http_check=True)
        host, port = _get_qdrant_host_port()
        client = QdrantClient(host=host, port=port, timeout=30, prefer_grpc=False)
        manager = QdrantSchemaManager(client)

        if cleanup_extra:
            cleanup_result = manager.cleanup_unknown_collections()
            if cleanup_result.get("deleted"):
                logger.info(f"已清理: {cleanup_result['deleted']}")

        results = manager.initialize_all_collections()
        success_count = sum(results.values())
        logger.info(f"Qdrant初始化: {success_count}/{len(results)}成功")
        return success_count > 0
    except Exception as e:
        logger.error(f"Qdrant初始化失败: {e}")
        return False


def get_qdrant_collections_info() -> Dict[str, Any]:
    from database.qdrant.connection import get_qdrant_client
    try:
        return QdrantSchemaManager(get_qdrant_client()).get_all_collections_info()
    except Exception as e:
        return {"error": str(e)}


def check_qdrant_collections_health() -> Dict[str, Any]:
    from database.qdrant.connection import get_qdrant_client
    try:
        return QdrantSchemaManager(get_qdrant_client()).health_check_all()
    except Exception as e:
        return {"error": str(e)}
