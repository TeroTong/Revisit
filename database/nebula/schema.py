import logging
import time
from config.settings import settings
from database.nebula.connection import execute_ngql

logger = logging.getLogger(__name__)

class NebulaSchemaManager:
    """NebulaGraph Schema管理器"""

    @staticmethod
    async def init_nebula_schema():
        """初始化NebulaGraph Schema"""
        logger.info("正在初始化NebulaGraph Schema...")

        try:

            # 首先测试连接
            from database.nebula.connection import test_nebula_connection
            if not test_nebula_connection():
                logger.error("NebulaGraph连接测试失败")
                return False

            # 检查并添加Storage节点（如果需要）
            if not await NebulaSchemaManager.ensure_and_add_storage_hosts():
                logger.error("Storage节点添加失败，无法继续初始化")
                return False

            # 创建图空间
            if not await NebulaSchemaManager.create_space():
                logger.error("图空间创建失败，无法继续初始化")
                return False

            # 使用图空间
            result = execute_ngql(f"USE {settings.DATABASE.NEBULA_SPACE}")
            if not result or not result.is_succeeded():
                error_msg = result.error_msg() if result else "Result is None"
                logger.error(f"使用图空间失败: {error_msg}")
                return False

            # 创建Tag
            await NebulaSchemaManager.create_tags()

            # 创建Edge
            await NebulaSchemaManager.create_edges()

            # 等待Schema同步 - 增加等待时间
            logger.info("等待Schema同步...")
            time.sleep(8)

            # 创建索引
            await NebulaSchemaManager.create_indexes()

            logger.info("✅ NebulaGraph Schema初始化完成")
            return True

        except Exception as e:
            logger.error(f"NebulaGraph Schema初始化失败: {e}", exc_info=True)
            return False

    @staticmethod
    async def ensure_and_add_storage_hosts():
        """检查并添加Storage节点（增强版：兼容不同数据结构）"""
        try:
            result = execute_ngql("SHOW HOSTS")
            if not result or not result.is_succeeded():
                error_msg = result.error_msg() if result else "Result is None"
                logger.error(f"获取主机列表失败: {error_msg}")
                return False

            # 更健壮的解析：兼容 row 作为 list 或其他数据结构
            has_online_storage = False
            logger.info("解析集群主机状态...")

            for i in range(result.row_size()):
                try:
                    row = result.row_values(i)
                    # 核心修复：安全地将 row 转换为字符串列表进行判断
                    row_str_list = []
                    # 方法1：如果 row 有 .size() 方法（旧版客户端可能）
                    if hasattr(row, 'size'):
                        for col in range(row.size()):
                            row_str_list.append(str(row[col]))
                    # 方法2：如果 row 是 list 或可迭代对象（新版客户端可能）
                    else:
                        for item in row:
                            row_str_list.append(str(item))

                    row_str = ' '.join(row_str_list).upper()

                    # 如果该行同时包含“STORAGE”和“ONLINE”，则认为有在线存储节点
                    if 'STORAGE' in row_str and 'ONLINE' in row_str:
                        has_online_storage = True
                        # 尝试提取主机地址（通常是第一个值）
                        host_addr = row_str_list[0] if row_str_list else 'unknown'
                        logger.info(f"  发现在线Storage节点: {host_addr}")
                        # 找到一个即可，无需继续
                        break

                except Exception as parse_e:
                    # 单行解析失败不影响整体检查，记录调试信息后继续
                    logger.debug(f"解析第 {i} 行数据时出现异常（可忽略）: {parse_e}")
                    continue

            if has_online_storage:
                logger.info("✅ 集群Storage节点状态正常。")
                return True
            else:
                # 在开发/单机环境下，如果连接正常但未检测到，记录警告并继续
                logger.warning("未明确检测到在线Storage节点，但连接正常。")
                logger.warning("在单机Docker部署中，此情况可能正常。将继续后续初始化。")
                # 根据您的环境决定是否放行，这里假设开发环境放行
                return settings.APP.ENVIRONMENT in ['development', 'test']

        except Exception as e:
            logger.error(f"检查Storage节点时出错: {e}", exc_info=True)
            return False

    @staticmethod
    async def create_space():
        """创建图空间"""
        max_retries = 3
        retry_count = 0

        # 先检查空间是否已存在
        check_query = f"DESCRIBE SPACE {settings.DATABASE.NEBULA_SPACE}"
        result = execute_ngql(check_query)
        if result and result.is_succeeded():
            logger.info(f"图空间 {settings.DATABASE.NEBULA_SPACE} 已存在")
            return True

        while retry_count < max_retries:
            # 增加分区数以适应更多节点和边
            query = f'''
                CREATE SPACE IF NOT EXISTS {settings.DATABASE.NEBULA_SPACE}
                (partition_num=50, replica_factor=1, vid_type=FIXED_STRING(128))
            '''

            result = execute_ngql(query)
            if result and result.is_succeeded():
                logger.info(f"✅ 图空间 {settings.DATABASE.NEBULA_SPACE} 创建成功")
                return True
            else:
                error_msg = result.error_msg()
                logger.warning(f"图空间创建失败 (尝试 {retry_count + 1}/{max_retries}): {error_msg}")

                # 检查是否是 Storage 相关错误
                if "StorageError" in error_msg or "E_RPC_FAILURE" in error_msg:
                    logger.error("检测到 Storage 节点问题，请手动执行: ADD HOSTS \"127.0.0.1\":9779")
                    logger.error("在 NebulaGraph 控制台中执行上述命令后重试")
                    break
                elif "SpaceNotFound" in error_msg:
                    logger.error("图空间不存在，可能 Storage 节点未正确添加")
                    break
                elif "already exists" in error_msg.lower():
                    logger.info(f"图空间 {settings.DATABASE.NEBULA_SPACE} 已存在")
                    return True
                else:
                    time.sleep(2)
                    retry_count += 1

        if retry_count >= max_retries:
            logger.error(f"图空间创建失败，已达到最大重试次数")

        return False

    @staticmethod
    async def create_tags():
        """创建5种Tag类型（去掉注释避免语法错误）"""
        tags = [
            # 1. 机构客户 Tag
            f'''
            CREATE TAG IF NOT EXISTS institution_customer (
                institution_customer_id string,
                customer_code string,
                name string,
                phone string,
                gender string,
                birthday string,
                created_at timestamp,
                updated_at timestamp
            )
            ''',

            # 2. 机构 Tag
            f'''
            CREATE TAG IF NOT EXISTS institution (
                institution_id string,
                institution_code string,
                name string,
                alias string,
                type string,
                status string,
                created_at timestamp,
                updated_at timestamp
            )
            ''',

            # 3. 医生 Tag
            f'''
            CREATE TAG IF NOT EXISTS doctor (
                doctor_id string,
                doctor_code string,
                name string,
                gender string,
                phone string,
                institution_code string,
                title string,
                specialty string,
                introduction string,
                created_at timestamp,
                updated_at timestamp
            )
            ''',

            # 4. 项目 Tag
            f'''
            CREATE TAG IF NOT EXISTS project (
                project_id string,
                project_code string,
                name string,
                category string,
                body_part string,
                risk_level int,
                indications string,
                contraindications string,
                description string,
                created_at timestamp,
                updated_at timestamp
            )
            ''',

            # 5. 产品 Tag
            f'''
            CREATE TAG IF NOT EXISTS product (
                product_id string,
                product_code string,
                name string,
                brand string,
                category string,
                body_part string,
                unit string,
                effect_level int,
                indications string,
                contraindications string,
                description string,
                created_at timestamp,
                updated_at timestamp
            )
            '''
        ]

        for tag_sql in tags:
            try:
                result = execute_ngql(tag_sql)
                if result and result.is_succeeded():
                    # 提取Tag名称
                    lines = tag_sql.strip().split('\n')
                    for line in lines:
                        if 'CREATE TAG IF NOT EXISTS' in line:
                            tag_name = line.split('CREATE TAG IF NOT EXISTS')[1].split()[0]
                            logger.info(f"✅ Tag创建成功: {tag_name}")
                            break
                else:
                    error_msg = result.error_msg() if result else "Result is None"
                    # 检查是否已存在
                    if "already exists" in error_msg.lower():
                        tag_name = tag_sql.split('CREATE TAG IF NOT EXISTS')[1].split()[0]
                        logger.info(f"Tag已存在: {tag_name}")
                    else:
                        logger.warning(f"Tag创建失败: {error_msg}")
            except Exception as e:
                logger.error(f"创建Tag时发生异常: {e}")

    @staticmethod
    async def create_edges():
        """创建15种Edge类型"""
        edges = [
            # 1. 机构用户节点 → 推荐了 → 机构用户节点
            f'''
            CREATE EDGE IF NOT EXISTS recommends (
                created_at timestamp,
                updated_at timestamp
            )
            ''',

            # 2. 项目节点 → 关联 → 项目节点
            f'''
            CREATE EDGE IF NOT EXISTS project_related_to_project (
                relation_type string,
                relation_level int,
                description string,
                created_at timestamp,
                updated_at timestamp
            )
            ''',

            # 3. 产品节点 → 关联 → 产品节点
            f'''
            CREATE EDGE IF NOT EXISTS product_related_to_product (
                relation_type string,
                relation_level int,
                description string,
                created_at timestamp,
                updated_at timestamp
            )
            ''',

            # 4. 项目节点 → 关联 → 产品节点
            f'''
            CREATE EDGE IF NOT EXISTS project_related_to_product (
                relation_type string,
                relation_level int,
                description string,
                created_at timestamp,
                updated_at timestamp
            )
            ''',

            # 5. 产品节点 → 关联 → 项目节点
            f'''
            CREATE EDGE IF NOT EXISTS product_related_to_project (
                relation_type string,
                relation_level int,
                description string,
                created_at timestamp,
                updated_at timestamp
            )
            ''',

            # 6. 机构节点 → 提供 → 项目节点
            f'''
            CREATE EDGE IF NOT EXISTS institution_provides_project (
                price double,
                is_available bool,
                available_from timestamp,
                available_to timestamp,
                created_at timestamp,
                updated_at timestamp
            )
            ''',

            # 7. 机构节点 → 提供 → 产品节点
            f'''
            CREATE EDGE IF NOT EXISTS institution_provides_product (
                stock int,
                price double,
                is_available bool,
                available_from timestamp,
                available_to timestamp,
                created_at timestamp,
                updated_at timestamp
            )
            ''',

            # 8. 医生节点 → 就职于 → 机构节点
            f'''
            CREATE EDGE IF NOT EXISTS doctor_works_at_institution (
                department string,
                status string,
                start_date timestamp,
                end_date timestamp,
                created_at timestamp,
                updated_at timestamp
            )
            ''',

            # 9. 医生节点 → 服务 → 机构用户节点
            f'''
            CREATE EDGE IF NOT EXISTS doctor_serves_customer (
                order_number string,
                order_date timestamp,
                order_time string,
                project_id string,
                product_id string,
                created_at timestamp,
                updated_at timestamp
            )
            ''',

            # 10. 机构用户节点 → 购买了 → 项目节点
            f'''
            CREATE EDGE IF NOT EXISTS customer_purchases_project (
                order_number string,
                order_date timestamp,
                order_time string,
                order_type string,
                current_times int,
                total_times int,
                actual_amount double,
                created_at timestamp,
                updated_at timestamp
            )
            ''',

            # 11. 机构用户节点 → 购买了 → 产品节点
            f'''
            CREATE EDGE IF NOT EXISTS customer_purchases_product (
                order_number string,
                order_date timestamp,
                order_time string,
                order_type string,
                current_times int,
                total_times int,
                actual_amount double,
                created_at timestamp,
                updated_at timestamp
            )
            ''',

            # 12. 机构用户节点 → 线上咨询了 → 机构节点
            f'''
            CREATE EDGE IF NOT EXISTS customer_consults_online (
                channel string,
                consultation_date timestamp,
                main_concern string,
                created_at timestamp,
                updated_at timestamp
            )
            ''',

            # 13. 机构用户节点 → 线下咨询了 → 机构节点
            f'''
            CREATE EDGE IF NOT EXISTS customer_consults_offline (
                consultation_date timestamp,
                consultation_type string,
                main_concern string,
                created_at timestamp,
                updated_at timestamp
            )
            ''',

            # 14. 机构用户节点 → 到店消费了 → 机构节点
            f'''
            CREATE EDGE IF NOT EXISTS customer_consumes_at_institution (
                order_number string,
                order_date timestamp,
                order_time string,
                order_type string,
                notes string,
                created_at timestamp,
                updated_at timestamp
            )
            ''',

            # 15. 机构用户节点 → 属于 → 机构节点
            f'''
            CREATE EDGE IF NOT EXISTS customer_belongs_to_institution (
                customer_code string,
                vip_level string,
                status string,
                total_consumption double,
                consumption_count int,
                created_at timestamp,
                updated_at timestamp
            )
            '''
        ]

        for edge_sql in edges:
            try:
                result = execute_ngql(edge_sql)
                if result and result.is_succeeded():
                    # 提取Edge名称
                    lines = edge_sql.strip().split('\n')
                    for line in lines:
                        if 'CREATE EDGE IF NOT EXISTS' in line:
                            edge_name = line.split('CREATE EDGE IF NOT EXISTS')[1].split()[0]
                            logger.info(f"✅ Edge创建成功: {edge_name}")
                            break
                else:
                    error_msg = result.error_msg() if result else "Result is None"
                    # 检查是否已存在
                    if "already exists" in error_msg.lower():
                        edge_name = edge_sql.split('CREATE EDGE IF NOT EXISTS')[1].split()[0]
                        logger.info(f"Edge已存在: {edge_name}")
                    else:
                        logger.warning(f"Edge创建失败: {error_msg}")
            except Exception as e:
                logger.error(f"创建Edge时发生异常: {e}")

    @staticmethod
    async def create_indexes():
        """创建索引 - 根据诊断结果优化"""
        logger.info("开始创建索引...")

        # 等待Schema完全同步
        import time
        logger.info("等待Schema完全同步...")
        time.sleep(10)

        success_count = 0
        fail_count = 0

        # Tag索引 - 为字符串字段指定长度
        tag_indexes = [
            # 字符串字段索引（必须指定长度）
            "CREATE TAG INDEX IF NOT EXISTS idx_institution_customer_id ON institution_customer(institution_customer_id(128))",
            "CREATE TAG INDEX IF NOT EXISTS idx_institution_id ON institution(institution_id(128))",
            "CREATE TAG INDEX IF NOT EXISTS idx_doctor_id ON doctor(doctor_id(128))",
            "CREATE TAG INDEX IF NOT EXISTS idx_project_id ON project(project_id(128))",
            "CREATE TAG INDEX IF NOT EXISTS idx_product_id ON product(product_id(128))",

            # 其他字符串字段索引（如果需要）
            "CREATE TAG INDEX IF NOT EXISTS idx_customer_code ON institution_customer(customer_code(64))",
            "CREATE TAG INDEX IF NOT EXISTS idx_customer_phone ON institution_customer(phone(32))",
            "CREATE TAG INDEX IF NOT EXISTS idx_doctor_code ON doctor(doctor_code(64))",

            # 整数字段索引（不需要长度）
            "CREATE TAG INDEX IF NOT EXISTS idx_project_risk ON project(risk_level)",
            "CREATE TAG INDEX IF NOT EXISTS idx_product_effect ON product(effect_level)",

            # 时间戳字段索引（不需要长度）
            "CREATE TAG INDEX IF NOT EXISTS idx_customer_created ON institution_customer(created_at)",
            "CREATE TAG INDEX IF NOT EXISTS idx_institution_created ON institution(created_at)",
            "CREATE TAG INDEX IF NOT EXISTS idx_doctor_created ON doctor(created_at)",
        ]

        # Edge索引 - 为字符串字段指定长度
        edge_indexes = [
            # 字符串字段索引（必须指定长度）
            "CREATE EDGE INDEX IF NOT EXISTS idx_purchases_order ON customer_purchases_project(order_number(128))",

            # 时间戳字段索引（不需要长度）
            "CREATE EDGE INDEX IF NOT EXISTS idx_purchases_date ON customer_purchases_project(order_date)",

            # 其他Edge索引
            "CREATE EDGE INDEX IF NOT EXISTS idx_consults_date ON customer_consults_online(consultation_date)",
            "CREATE EDGE INDEX IF NOT EXISTS idx_consumes_date ON customer_consumes_at_institution(order_date)",

            # 字符串字段Edge索引
            "CREATE EDGE INDEX IF NOT EXISTS idx_belongs_status ON customer_belongs_to_institution(status(32))",
            "CREATE EDGE INDEX IF NOT EXISTS idx_belongs_customer_code ON customer_belongs_to_institution(customer_code(64))",
        ]

        # 先创建Tag索引，再创建Edge索引
        all_indexes = tag_indexes + edge_indexes

        for index_sql in all_indexes:
            try:
                logger.debug(f"正在创建索引: {index_sql[:80]}...")
                result = execute_ngql(index_sql)

                if result is not None:
                    if hasattr(result, 'is_succeeded'):
                        if result.is_succeeded():
                            # 提取索引名称用于日志
                            if "CREATE TAG INDEX" in index_sql:
                                idx_name = index_sql.split("CREATE TAG INDEX IF NOT EXISTS ")[1].split(" ON")[0]
                            elif "CREATE EDGE INDEX" in index_sql:
                                idx_name = index_sql.split("CREATE EDGE INDEX IF NOT EXISTS ")[1].split(" ON")[0]
                            else:
                                idx_name = "unknown"

                            logger.info(f"✅ 索引创建成功: {idx_name}")
                            success_count += 1
                        else:
                            error_msg = result.error_msg()

                            # 处理常见错误
                            if "already exists" in error_msg.lower() or "existed" in error_msg.lower():
                                logger.debug(f"索引已存在: {error_msg}")
                                success_count += 1  # 已存在也算成功
                            elif "Invalid param" in error_msg:
                                logger.warning(f"❌ 参数无效（可能字段类型不支持索引）: {error_msg}")
                                fail_count += 1
                            else:
                                logger.warning(f"⚠️ 索引创建失败: {error_msg}")
                                fail_count += 1
                    else:
                        logger.warning(f"⚠️ 返回结果无is_succeeded属性")
                        fail_count += 1
                else:
                    logger.warning(f"⚠️ 执行返回None")
                    fail_count += 1

                # 每个索引之间等待一下
                time.sleep(1)

            except Exception as e:
                logger.error(f"❌ 执行索引创建语句时发生异常: {e}")
                fail_count += 1

        # 在NebulaGraph中，CREATE INDEX成功后索引就已经可用
        # BUILD INDEX命令是可选的，用于加速索引可用性
        # 对于NebulaGraph 3.8.0，如果需要构建索引，可以使用单独的脚本

        # 最终统计
        logger.info(f"索引创建统计: {success_count}成功, {fail_count}失败")

        if fail_count == 0:
            logger.info("✅ 所有索引创建成功")
            return True
        elif success_count > 0:
            logger.warning(f"⚠️ 部分索引创建失败 ({fail_count}/{len(all_indexes)})")
            return True  # 部分成功也算成功
        else:
            logger.error("❌ 所有索引创建失败")
            return False

    @staticmethod
    async def drop_schema():
        """删除Schema"""
        try:
            # 删除图空间
            query = f"DROP SPACE IF EXISTS {settings.DATABASE.NEBULA_SPACE}"
            result = execute_ngql(query)

            if result and result.is_succeeded():
                logger.info(f"✅ 图空间 {settings.DATABASE.NEBULA_SPACE} 已删除")
                return True
            else:
                logger.error(f"删除图空间失败: {result.error_msg()}")
                return False
        except Exception as e:
            logger.error(f"删除Schema失败: {e}")
            return False


# 方便使用的函数
async def init_nebula_schema():
    """初始化NebulaGraph Schema"""
    manager = NebulaSchemaManager()
    return await manager.init_nebula_schema()


async def create_tags():
    """创建Tag"""
    manager = NebulaSchemaManager()
    await manager.create_tags()


async def create_edges():
    """创建Edge"""
    manager = NebulaSchemaManager()
    await manager.create_edges()