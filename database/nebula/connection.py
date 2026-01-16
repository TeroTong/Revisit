"""
NebulaGraph连接管理
专门处理Docker Compose部署的集群初始化问题
"""
import logging
import time
from typing import List, Tuple, Optional
from nebula3.Config import Config
from nebula3.gclient.net import ConnectionPool

from config.settings import settings

logger = logging.getLogger(__name__)


class NebulaConnection:
    """NebulaGraph连接管理器"""

    _connection_pool = None
    _session = None
    _initialized = False

    @classmethod
    def init_connection(cls):
        """初始化连接池"""
        if cls._connection_pool is not None:
            return cls._connection_pool

        try:
            logger.info(f"正在连接NebulaGraph: {settings.DATABASE.NEBULA_HOST}:{settings.DATABASE.NEBULA_PORT}")

            # 创建配置
            config = Config()
            config.max_connection_pool_size = 10
            config.timeout = 10000  # 10秒超时

            # 创建连接池
            cls._connection_pool = ConnectionPool()

            # 初始化连接池
            success = cls._connection_pool.init(
                [(settings.DATABASE.NEBULA_HOST, settings.DATABASE.NEBULA_PORT)],
                config
            )

            if not success:
                raise Exception("连接池初始化失败")

            logger.info("✅ NebulaGraph连接池创建成功")
            return cls._connection_pool

        except Exception as e:
            logger.error(f"❌ NebulaGraph连接池创建失败: {e}")
            raise

    @classmethod
    def get_session(cls):
        """获取会话"""
        if cls._session is None:
            cls.init_connection()

            try:
                # 获取会话
                cls._session = cls._connection_pool.get_session(
                    settings.DATABASE.NEBULA_USER,
                    settings.DATABASE.NEBULA_PASSWORD
                )
                logger.info("✅ NebulaGraph会话创建成功")
            except Exception as e:
                logger.error(f"❌ 创建会话失败: {e}")
                raise

        return cls._session

    @classmethod
    def close_session(cls):
        """关闭会话"""
        if cls._session:
            try:
                cls._session.release()
                logger.info("✅ NebulaGraph会话已关闭")
            except Exception as e:
                logger.warning(f"关闭会话时出错: {e}")
            finally:
                cls._session = None

    @classmethod
    def close_connection_pool(cls):
        """关闭连接池"""
        if cls._connection_pool:
            try:
                cls._connection_pool.close()
                logger.info("✅ NebulaGraph连接池已关闭")
            except Exception as e:
                logger.warning(f"关闭连接池时出错: {e}")
            finally:
                cls._connection_pool = None

    @classmethod
    def execute_query(cls, query: str):
        """执行nGQL查询"""
        try:
            session = cls.get_session()
            result = session.execute(query)

            if not result.is_succeeded():
                logger.error(f"查询执行失败: {query}")
                logger.error(f"错误信息: {result.error_msg()}")
                return None

            return result
        except Exception as e:
            logger.error(f"执行查询时发生异常: {e}")
            return None

    @classmethod
    def check_cluster_status(cls) -> Tuple[bool, int]:
        """
        检查集群状态
        返回: (是否成功, 主机数量)
        """
        try:
            result = cls.execute_query("SHOW HOSTS")
            if result is None:
                return False, 0

            host_count = result.row_size()
            logger.info(f"当前集群主机数量: {host_count}")

            # 显示主机状态
            if host_count > 0:
                logger.info("集群主机状态:")
                for i in range(host_count):
                    try:
                        row = result.row_values(i)
                        if len(row) >= 3:
                            host = row[0].as_string()
                            port = row[1].as_int()
                            status = row[2].as_string()
                            logger.info(f"  {host}:{port} - {status}")
                    except Exception as e:
                        logger.debug(f"解析主机信息出错: {e}")

            return True, host_count

        except Exception as e:
            logger.error(f"检查集群状态失败: {e}")
            return False, 0

    @classmethod
    def add_storage_hosts(cls, storage_hosts: Optional[List[Tuple[str, int]]] = None):
        """
        添加Storage主机到集群

        Args:
            storage_hosts: Storage主机列表，格式: [("host1", port1), ("host2", port2)]
                        如果为None，使用默认配置
        """
        try:
            # 如果没有提供主机列表，使用默认配置
            if storage_hosts is None:
                # 尝试常见配置
                possible_hosts = [
                    ("storaged", 9779),       # 单节点
                    ("nebula-storaged", 9779), # 带前缀
                    ("storaged0", 9779),      # 多节点
                    ("storaged1", 9779),
                    ("storaged2", 9779),
                ]
                storage_hosts = possible_hosts[:1]  # 默认使用第一个

            # 构建ADD HOSTS命令
            hosts_str = ", ".join([f'"{host}":{port}' for host, port in storage_hosts])
            query = f"ADD HOSTS {hosts_str};"

            logger.info(f"添加Storage主机: {query}")
            result = cls.execute_query(query)

            if result is None:
                logger.error("添加Storage主机失败")
                return False

            # 检查是否添加成功
            if result.is_succeeded():
                logger.info("✅ Storage主机添加成功")

                # 等待集群同步
                logger.info("等待集群同步...")
                time.sleep(20)

                # 验证添加结果
                success, host_count = cls.check_cluster_status()
                if success and host_count > 0:
                    logger.info(f"✅ 集群现在有 {host_count} 个主机")
                    cls._initialized = True
                    return True
                else:
                    logger.warning("⚠️ 集群状态检查失败，可能添加未完全生效")
                    return False
            else:
                error_msg = result.error_msg()
                logger.error(f"❌ 添加Storage主机失败: {error_msg}")

                # 如果错误是"主机已存在"，也算成功
                if "already added" in error_msg.lower() or "exist" in error_msg.lower():
                    logger.info("主机已存在，继续执行...")
                    cls._initialized = True
                    return True

                return False

        except Exception as e:
            logger.error(f"添加Storage主机时发生异常: {e}")
            return False

    @classmethod
    def create_space_if_not_exists(cls, space_name: str):
        """创建图空间（如果不存在）"""
        try:
            # 检查空间是否已存在
            check_query = f"DESCRIBE SPACE {space_name}"
            check_result = cls.execute_query(check_query)

            if check_result is not None and check_result.is_succeeded():
                logger.info(f"✅ 图空间 '{space_name}' 已存在")
                return True

            # 空间不存在，创建新空间
            create_query = (
                f"CREATE SPACE IF NOT EXISTS {space_name}("
                f"vid_type=FIXED_STRING(128), "
                f"partition_num=1, "
                f"replica_factor=1)"
            )

            logger.info(f"创建图空间: {create_query}")
            create_result = cls.execute_query(create_query)

            if create_result is None:
                logger.error("创建图空间失败")
                return False

            if create_result.is_succeeded():
                logger.info(f"✅ 图空间 '{space_name}' 创建成功")

                # 等待空间创建完成
                logger.info("等待图空间创建完成...")
                time.sleep(10)

                # 使用新创建的空间
                use_result = cls.execute_query(f"USE {space_name}")
                if use_result is not None and use_result.is_succeeded():
                    logger.info(f"✅ 成功使用图空间: {space_name}")
                    return True
                else:
                    logger.warning(f"使用图空间失败: {space_name}")
                    return False
            else:
                error_msg = create_result.error_msg()
                logger.error(f"❌ 创建图空间失败: {error_msg}")
                return False

        except Exception as e:
            logger.error(f"创建图空间时发生异常: {e}")
            return False

    @classmethod
    def initialize_cluster(cls, space_name: str = None):
        """
        初始化NebulaGraph集群（智能版）
        1. 检查集群状态
        2. 仅在必要时添加Storage主机
        3. 创建图空间
        """
        try:
            if cls._initialized:
                logger.info("集群已经初始化")
                return True

            # 1. 检查集群状态
            logger.info("正在检查NebulaGraph集群状态...")
            success, host_count = cls.check_cluster_status()

            if not success:
                logger.error("无法检查集群状态，连接可能有问题")
                return False

            # 2. 智能判断：只有当集群为空时才尝试添加主机
            needs_initialization = False

            if host_count == 0:
                logger.warning("⚠️ 集群中没有Storage主机，将尝试添加...")
                needs_initialization = True
            else:
                # 检查是否有在线的Storage主机
                logger.info(f"集群中已有 {host_count} 个主机，检查状态...")
                result = cls.execute_query("SHOW HOSTS")
                if result and result.is_succeeded():
                    online_hosts = 0
                    offline_hosts = 0

                    for i in range(result.row_size()):
                        try:
                            row = result.row_values(i)
                            if len(row) >= 3:
                                status = row[2].as_string()
                                if status == "ONLINE":
                                    online_hosts += 1
                                elif status == "OFFLINE":
                                    offline_hosts += 1
                        except Exception as e:
                            logger.debug(f"解析主机状态时出错: {e}")

                    logger.info(f"在线主机: {online_hosts}, 离线主机: {offline_hosts}")

                    # 如果所有主机都是离线的，仍然需要初始化
                    if online_hosts == 0 and offline_hosts > 0:
                        logger.warning("所有主机状态为OFFLINE，将尝试重新添加...")
                        needs_initialization = True
                        # 先清理离线主机（可选）
                        # for i in range(result.row_size()):
                        #     try:
                        #         row = result.row_values(i)
                        #         host = row[0].as_string()
                        #         port = row[1].as_int()
                        #         drop_cmd = f'DROP HOSTS "{host}:{port}"'
                        #         cls.execute_query(drop_cmd)
                        #     except:
                        #         pass

            # 3. 仅在需要时添加Storage主机
            if needs_initialization:
                logger.info("执行集群初始化...")
                if not cls.add_storage_hosts():
                    logger.error("❌ 添加Storage主机失败")
                    return False
            else:
                logger.info("✅ 集群已就绪，跳过添加主机步骤")
                cls._initialized = True

            # 4. 创建图空间（如果指定了空间名）
            if space_name:
                if not cls.create_space_if_not_exists(space_name):
                    logger.error("❌ 创建图空间失败")
                    return False

            logger.info("✅ NebulaGraph集群初始化完成")
            return True

        except Exception as e:
            logger.error(f"初始化集群时发生异常: {e}")
            return False

    @classmethod
    def initialize_with_config(cls, space_name: str = None, auto_add_hosts: bool = None):
        """
        根据配置初始化集群

        Args:
            space_name: 图空间名称
            auto_add_hosts: 是否自动添加主机，None表示从配置读取
        """
        # 从配置读取设置
        if auto_add_hosts is None:
            try:
                from config.settings import settings
                if hasattr(settings.DATABASE, 'NEBULA_AUTO_ADD_HOSTS'):
                    auto_add_hosts = settings.DATABASE.NEBULA_AUTO_ADD_HOSTS
                else:
                    auto_add_hosts = True  # 默认值，保持兼容
            except:
                auto_add_hosts = True

        logger.info(f"集群初始化模式: {'自动添加主机' if auto_add_hosts else '仅检查状态'}")

        if auto_add_hosts:
            # 使用完整的初始化流程（包含ADD HOSTS）
            return cls.initialize_cluster(space_name)
        else:
            # 简化流程：仅检查状态和创建空间
            try:
                logger.info("使用简化初始化流程（跳过ADD HOSTS）...")

                # 1. 检查连接
                if not test_nebula_connection():
                    return False

                # 2. 检查集群状态
                success, host_count = cls.check_cluster_status()
                if not success:
                    return False

                if host_count == 0:
                    logger.warning("集群为空，但配置为不自动添加主机。请手动执行ADD HOSTS。")
                    # 这里可以返回False或True，取决于你的需求
                    return False

                # 3. 创建空间
                if space_name and not cls.create_space_if_not_exists(space_name):
                    return False

                cls._initialized = True
                logger.info("✅ 简化初始化完成")
                return True

            except Exception as e:
                logger.error(f"简化初始化失败: {e}")
                return False


# 全局函数 - 保持向后兼容
def get_nebula_session():
    """获取NebulaGraph会话"""
    return NebulaConnection.get_session()

async def get_nebula_session_async():
    """异步获取NebulaGraph会话"""
    return NebulaConnection.get_session()

def close_nebula_session():
    """关闭NebulaGraph会话（保持向后兼容）"""
    NebulaConnection.close_session()
    NebulaConnection.close_connection_pool()

# 同时提供新版本函数名，便于记忆
close_nebula_connections = close_nebula_session

def execute_ngql(query: str):
    """执行nGQL查询"""
    return NebulaConnection.execute_query(query)

def test_nebula_connection():
    """测试NebulaGraph连接"""
    try:
        # 简单测试连接
        session = get_nebula_session()
        result = session.execute("YIELD 1")

        if result.is_succeeded():
            logger.info("✅ NebulaGraph连接测试成功")
            return True
        else:
            logger.error(f"连接测试失败: {result.error_msg()}")
            return False
    except Exception as e:
        logger.error(f"连接测试异常: {e}")
        return False

def initialize_nebula_graph(space_name: str = None):
    """初始化NebulaGraph集群（同步版本）"""
    if space_name is None:
        from config.settings import settings
        space_name = settings.DATABASE.NEBULA_SPACE

    return NebulaConnection.initialize_cluster(space_name)