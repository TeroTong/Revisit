"""
ClickHouse Schema定义

设计原则：
1. ClickHouse 用于 OLAP 分析，从 PostgreSQL 同步数据
2. 采用宽表设计，减少 JOIN 操作
3. 按时间分区，便于数据管理和查询优化
4. 针对医美回访系统的分析场景优化

数据流向：PostgreSQL (主数据) -> ClickHouse (分析数据)
"""
import logging
from database.clickhouse.connection import execute_query
from config.settings import settings

logger = logging.getLogger(__name__)


class ClickHouseSchemaManager:
    """ClickHouse Schema管理器"""

    @staticmethod
    async def init_clickhouse_schema():
        """初始化ClickHouse Schema"""
        logger.info("正在初始化ClickHouse Schema...")

        try:
            # 创建数据库
            await ClickHouseSchemaManager.create_database()

            # 创建基础维度表
            await ClickHouseSchemaManager._create_dimension_tables()

            # 创建事实表（业务数据）
            await ClickHouseSchemaManager._create_fact_tables()

            # 创建聚合分析表
            await ClickHouseSchemaManager._create_analytics_tables()

            logger.info("✅ ClickHouse Schema初始化完成")
            return True

        except Exception as e:
            logger.error(f"ClickHouse Schema初始化失败: {e}")
            return False

    @staticmethod
    async def create_database():
        """创建数据库"""
        query = f"CREATE DATABASE IF NOT EXISTS {settings.DATABASE.CLICKHOUSE_DB}"

        try:
            await execute_query(query)
            logger.info(f"✅ 数据库 {settings.DATABASE.CLICKHOUSE_DB} 创建成功")
        except Exception as e:
            logger.warning(f"数据库创建失败: {e}")

    # ==================== 维度表 ====================

    @staticmethod
    async def _create_dimension_tables():
        """创建维度表"""
        await ClickHouseSchemaManager._create_dim_institution_table()
        await ClickHouseSchemaManager._create_dim_project_table()
        await ClickHouseSchemaManager._create_dim_product_table()
        await ClickHouseSchemaManager._create_dim_doctor_table()
        await ClickHouseSchemaManager._create_dim_customer_table()

    @staticmethod
    async def _create_dim_institution_table():
        """机构维度表"""
        query = f'''
            CREATE TABLE IF NOT EXISTS {settings.DATABASE.CLICKHOUSE_DB}.dim_institution (
                institution_id String,
                institution_code String,
                name String,
                alias Nullable(String),
                type Nullable(String),
                status String DEFAULT 'ACTIVE',
                created_at DateTime DEFAULT now(),
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY institution_id
        '''
        try:
            await execute_query(query)
            logger.info("✅ 机构维度表创建完成")
        except Exception as e:
            logger.error(f"创建机构维度表失败: {e}")

    @staticmethod
    async def _create_dim_project_table():
        """项目维度表"""
        query = f'''
            CREATE TABLE IF NOT EXISTS {settings.DATABASE.CLICKHOUSE_DB}.dim_project (
                project_id String,
                project_code String,
                name String,
                category Nullable(String),
                body_part Nullable(String),
                risk_level Nullable(Int8),
                indications Nullable(String),
                contraindications Nullable(String),
                description Nullable(String),
                created_at DateTime DEFAULT now(),
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY project_id
        '''
        try:
            await execute_query(query)
            logger.info("✅ 项目维度表创建完成")
        except Exception as e:
            logger.error(f"创建项目维度表失败: {e}")

    @staticmethod
    async def _create_dim_product_table():
        """产品维度表"""
        query = f'''
            CREATE TABLE IF NOT EXISTS {settings.DATABASE.CLICKHOUSE_DB}.dim_product (
                product_id String,
                product_code String,
                name String,
                
                brand Nullable(String),
                category Nullable(String),
                body_part Nullable(String),
                unit Nullable(String),
                effect_level Nullable(Int8),
                
                indications Nullable(String),
                contraindications Nullable(String),
                description Nullable(String),
                
                created_at DateTime DEFAULT now(),
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY product_id
        '''
        try:
            await execute_query(query)
            logger.info("✅ 产品维度表创建完成")
        except Exception as e:
            logger.error(f"创建产品维度表失败: {e}")

    @staticmethod
    async def _create_dim_doctor_table():
        """医生维度表"""
        query = f'''
            CREATE TABLE IF NOT EXISTS {settings.DATABASE.CLICKHOUSE_DB}.dim_doctor (
                doctor_id String,
                doctor_code String,
                name String,
                
                gender Nullable(String),
                phone Nullable(String),
                
                institution_code Nullable(String),
                title Nullable(String),
                specialty Array(String) DEFAULT [],
                introduction Nullable(String),
                
                created_at DateTime DEFAULT now(),
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY doctor_id
        '''
        try:
            await execute_query(query)
            logger.info("✅ 医生维度表创建完成")
        except Exception as e:
            logger.error(f"创建医生维度表失败: {e}")

    @staticmethod
    async def _create_dim_customer_table():
        """客户维度宽表（合并自然人和机构客户信息）"""
        query = f'''
            CREATE TABLE IF NOT EXISTS {settings.DATABASE.CLICKHOUSE_DB}.dim_customer (
                -- 主键
                institution_customer_id String,
                
                -- 自然人信息
                person_id String,
                customer_code String,
                name String,
                phone String,
                gender Nullable(String),
                birthday Nullable(Date),
                
                -- 机构客户信息
                institution_id String,
                institution_code String,
                vip_level String DEFAULT 'NORMAL',
                status String DEFAULT 'ACTIVE',
                
                -- 消费统计
                first_visit_date Nullable(Date),
                last_visit_date Nullable(Date),
                consumption_count Int32 DEFAULT 0,
                total_consumption Decimal(12, 2) DEFAULT 0,
                
                -- 关联信息
                referrer_id Nullable(String),
                doctor_id Nullable(String),
                
                -- 时间戳
                created_at DateTime DEFAULT now(),
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (institution_code, institution_customer_id)
        '''
        try:
            await execute_query(query)
            logger.info("✅ 客户维度宽表创建完成")
        except Exception as e:
            logger.error(f"创建客户维度宽表失败: {e}")

    # ==================== 事实表 ====================

    @staticmethod
    async def _create_fact_tables():
        """创建事实表"""
        await ClickHouseSchemaManager._create_fact_consumption_table()
        await ClickHouseSchemaManager._create_fact_consultation_table()
        await ClickHouseSchemaManager._create_fact_reminder_table()

    @staticmethod
    async def _create_fact_consumption_table():
        """消费记录事实表（宽表设计，包含关联维度信息）"""
        query = f'''
            CREATE TABLE IF NOT EXISTS {settings.DATABASE.CLICKHOUSE_DB}.fact_consumption (
                -- 主键
                consumption_id String,
                
                -- 时间维度
                order_date Date,
                order_time Nullable(String),
                order_year UInt16 MATERIALIZED toYear(order_date),
                order_month UInt8 MATERIALIZED toMonth(order_date),
                order_week UInt8 MATERIALIZED toISOWeek(order_date),
                order_day_of_week UInt8 MATERIALIZED toDayOfWeek(order_date),
                
                -- 机构维度
                institution_id String,
                institution_code String,
                institution_name String,
                
                -- 客户维度
                institution_customer_id String,
                customer_code String,
                customer_name String,
                customer_phone String,
                customer_gender Nullable(String),
                customer_vip_level String,
                customer_birthday Nullable(Date),
                
                -- 项目/产品维度
                project_id Nullable(String),
                project_code Nullable(String),
                project_name Nullable(String),
                project_category Nullable(String),
                project_risk_level Nullable(Int8),
                product_id Nullable(String),
                product_code Nullable(String),
                product_name Nullable(String),
                product_brand Nullable(String),
                product_category Nullable(String),
                product_effect_level Nullable(Int8),
                
                -- 医生维度
                doctor_id Nullable(String),
                doctor_code Nullable(String),
                doctor_name Nullable(String),
                
                -- 订单信息
                order_number String,
                order_type Nullable(String),
                current_times Int32 DEFAULT 1,
                total_times Int32 DEFAULT 1,
                
                -- 金额信息
                total_amount Decimal(12, 2),
                discount_amount Decimal(12, 2) DEFAULT 0,
                actual_amount Decimal(12, 2),
                payment_method Nullable(String),
                payment_status String DEFAULT 'PAID',
                
                -- 退款信息
                is_refund UInt8 DEFAULT 0,
                refund_amount Nullable(Decimal(12, 2)),
                refund_reason Nullable(String),
                
                -- 备注
                notes Nullable(String),
                
                -- 时间戳
                created_at DateTime DEFAULT now(),
                updated_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(order_date)
            ORDER BY (institution_code, order_date, institution_customer_id)
        '''
        try:
            await execute_query(query)
            logger.info("✅ 消费记录事实表创建完成")
        except Exception as e:
            logger.error(f"创建消费记录事实表失败: {e}")

    @staticmethod
    async def _create_fact_consultation_table():
        """咨询记录事实表（合并线上线下咨询）

        设计说明：
        - 合并 online_consultation_* 和 offline_consultation_* 表
        - 线上咨询 (ONLINE) 包含会话统计字段
        - 线下咨询 (OFFLINE) 会话统计字段为空/默认值
        """
        query = f'''
            CREATE TABLE IF NOT EXISTS {settings.DATABASE.CLICKHOUSE_DB}.fact_consultation (
                -- 主键
                consultation_id String,
                
                -- 咨询类型：ONLINE/OFFLINE
                consultation_type String,
                
                -- 时间维度
                consultation_date Date,
                consultation_year UInt16 MATERIALIZED toYear(consultation_date),
                consultation_month UInt8 MATERIALIZED toMonth(consultation_date),
                consultation_week UInt8 MATERIALIZED toISOWeek(consultation_date),
                consultation_day_of_week UInt8 MATERIALIZED toDayOfWeek(consultation_date),
                
                -- 机构维度
                institution_id String,
                institution_code String,
                institution_name String,
                
                -- 客户维度
                institution_customer_id String,
                customer_code String,
                customer_name String,
                customer_vip_level String,
                
                -- 咨询信息
                channel Nullable(String),
                main_concern Nullable(String),
                
                -- ========== 以下字段仅线上咨询 (ONLINE) 有值 ==========
                -- 会话时间
                session_start_time Nullable(DateTime),
                session_end_time Nullable(DateTime),
                session_duration_minutes Int32 DEFAULT 0,
                
                -- 消息统计
                total_message_count Int32 DEFAULT 0,
                customer_message_count Int32 DEFAULT 0,
                institution_message_count Int32 DEFAULT 0,
                system_message_count Int32 DEFAULT 0,
                
                -- 响应统计
                first_response_seconds Nullable(Int32),
                avg_response_seconds Nullable(Int32),
                
                -- 时间戳
                created_at DateTime DEFAULT now(),
                updated_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(consultation_date)
            ORDER BY (institution_code, consultation_date, consultation_type)
        '''
        try:
            await execute_query(query)
            logger.info("✅ 咨询记录事实表创建完成")
        except Exception as e:
            logger.error(f"创建咨询记录事实表失败: {e}")

    @staticmethod
    async def _create_fact_reminder_table():
        """回访记录事实表

        数据来源：
        - birthday_reminder_* 表：生日回访记录
        - reminder_record_* 表：其他类型回访记录

        reminder_type 回访类型说明：
        - BIRTHDAY: 生日回访（来自 birthday_reminder_* 表）
        - TREATMENT_FOLLOWUP: 治疗后回访
        - PERIODIC_CARE: 定期关怀
        - PROMOTION: 活动推广
        - OTHER: 其他类型
        """
        query = f'''
            CREATE TABLE IF NOT EXISTS {settings.DATABASE.CLICKHOUSE_DB}.fact_reminder (
                -- 主键
                reminder_id String,
                
                -- 回访类型（见上方说明）
                reminder_type String,
                
                -- 时间维度（计划回访日期）
                reminder_date Date,
                reminder_year UInt16 MATERIALIZED toYear(reminder_date),
                reminder_month UInt8 MATERIALIZED toMonth(reminder_date),
                reminder_week UInt8 MATERIALIZED toISOWeek(reminder_date),
                reminder_day_of_week UInt8 MATERIALIZED toDayOfWeek(reminder_date),
                
                -- 完成日期
                complete_date Nullable(Date),
                
                -- 机构维度
                institution_id String,
                institution_code String,
                institution_name String,
                
                -- 客户维度（基本信息，详细信息通过 dim_customer 关联）
                institution_customer_id String,
                customer_code String,
                customer_name String,
                customer_vip_level String,
                
                -- 状态信息
                reminder_status String DEFAULT 'PENDING',
                
                -- 时间戳
                created_at DateTime DEFAULT now(),
                updated_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(reminder_date)
            ORDER BY (institution_code, reminder_date, reminder_type)
        '''
        try:
            await execute_query(query)
            logger.info("✅ 回访记录事实表创建完成")
        except Exception as e:
            logger.error(f"创建回访记录事实表失败: {e}")


    # ==================== 聚合分析表 ====================

    @staticmethod
    async def _create_analytics_tables():
        """创建聚合分析表"""
        await ClickHouseSchemaManager._create_daily_consumption_stats_table()
        await ClickHouseSchemaManager._create_daily_reminder_stats_table()
        await ClickHouseSchemaManager._create_customer_rfm_table()
        await ClickHouseSchemaManager._create_project_analysis_table()
        await ClickHouseSchemaManager._create_product_analysis_table()

    @staticmethod
    async def _create_daily_consumption_stats_table():
        """每日消费统计表

        设计说明：
        - 只按 机构+日期 聚合，保持表简洁
        - 如需按项目分类/产品品牌/VIP等级分析，直接查询 fact_consumption 动态聚合
        - 避免维度组合爆炸问题
        - 咨询转化率需要跨表关联，建议在查询时动态计算
        """
        query = f'''
            CREATE TABLE IF NOT EXISTS {settings.DATABASE.CLICKHOUSE_DB}.agg_daily_consumption (
                -- 统计日期
                stat_date Date,
                
                -- 维度
                institution_code String,
                
                -- 订单指标
                order_count Int64 DEFAULT 0,
                customer_count Int64 DEFAULT 0,
                new_customer_count Int64 DEFAULT 0,
                
                -- 金额指标
                total_amount Decimal(18, 2) DEFAULT 0,
                actual_amount Decimal(18, 2) DEFAULT 0,
                discount_amount Decimal(18, 2) DEFAULT 0,
                avg_order_amount Decimal(12, 2) DEFAULT 0,
                
                -- 退款指标
                refund_count Int64 DEFAULT 0,
                refund_amount Decimal(18, 2) DEFAULT 0,
                
                -- 时间戳
                created_at DateTime DEFAULT now()
            ) ENGINE = SummingMergeTree()
            PARTITION BY toYYYYMM(stat_date)
            ORDER BY (stat_date, institution_code)
        '''
        try:
            await execute_query(query)
            logger.info("✅ 每日消费统计表创建完成")
        except Exception as e:
            logger.error(f"创建每日消费统计表失败: {e}")

    @staticmethod
    async def _create_daily_reminder_stats_table():
        """每日回访统计表"""
        query = f'''
            CREATE TABLE IF NOT EXISTS {settings.DATABASE.CLICKHOUSE_DB}.agg_daily_reminder (
                -- 统计日期
                stat_date Date,
                
                -- 维度
                institution_code String,
                reminder_type String,
                
                -- 指标
                total_count Int64 DEFAULT 0,
                pending_count Int64 DEFAULT 0,
                completed_count Int64 DEFAULT 0,
                cancelled_count Int64 DEFAULT 0,
                completion_rate Float64 DEFAULT 0,
                
                -- 时间戳
                created_at DateTime DEFAULT now()
            ) ENGINE = SummingMergeTree()
            PARTITION BY toYYYYMM(stat_date)
            ORDER BY (stat_date, institution_code, reminder_type)
        '''
        try:
            await execute_query(query)
            logger.info("✅ 每日回访统计表创建完成")
        except Exception as e:
            logger.error(f"创建每日回访统计表失败: {e}")

    @staticmethod
    async def _create_customer_rfm_table():
        """客户RFM分析表"""
        query = f'''
            CREATE TABLE IF NOT EXISTS {settings.DATABASE.CLICKHOUSE_DB}.agg_customer_rfm (
                -- 分析日期
                analysis_date Date,
                
                -- 客户信息
                institution_code String,
                institution_customer_id String,
                customer_code String,
                customer_name String,
                customer_vip_level String,
                
                -- RFM指标
                -- Recency: 最近一次消费距今天数
                recency_days Int32 DEFAULT 0,
                recency_score Int8 DEFAULT 0,
                
                -- Frequency: 消费频次
                frequency_count Int32 DEFAULT 0,
                frequency_score Int8 DEFAULT 0,
                
                -- Monetary: 消费金额
                monetary_amount Decimal(12, 2) DEFAULT 0,
                monetary_score Int8 DEFAULT 0,
                
                -- RFM综合
                rfm_score Int16 DEFAULT 0,
                rfm_segment String DEFAULT 'UNKNOWN',
                
                -- 其他指标
                first_purchase_date Nullable(Date),
                last_purchase_date Nullable(Date),
                avg_order_amount Decimal(12, 2) DEFAULT 0,
                
                -- 时间戳
                created_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(created_at)
            PARTITION BY toYYYYMM(analysis_date)
            ORDER BY (analysis_date, institution_code, institution_customer_id)
        '''
        try:
            await execute_query(query)
            logger.info("✅ 客户RFM分析表创建完成")
        except Exception as e:
            logger.error(f"创建客户RFM分析表失败: {e}")

    @staticmethod
    async def _create_project_analysis_table():
        """项目分析表"""
        query = f'''
            CREATE TABLE IF NOT EXISTS {settings.DATABASE.CLICKHOUSE_DB}.agg_project_analysis (
                -- 分析日期
                analysis_date Date,
                
                -- 项目信息
                institution_code String,
                project_id String,
                project_code String,
                project_name String,
                project_category Nullable(String),
                
                -- 销售指标
                sale_count Int64 DEFAULT 0,
                sale_amount Decimal(18, 2) DEFAULT 0,
                customer_count Int64 DEFAULT 0,
                new_customer_count Int64 DEFAULT 0,
                repeat_customer_count Int64 DEFAULT 0,
                
                -- 复购率 = repeat_customer_count / customer_count
                repeat_purchase_rate Float64 DEFAULT 0,
                
                -- 时间戳
                created_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(created_at)
            PARTITION BY toYYYYMM(analysis_date)
            ORDER BY (analysis_date, institution_code, project_id)
        '''
        try:
            await execute_query(query)
            logger.info("✅ 项目分析表创建完成")
        except Exception as e:
            logger.error(f"创建项目分析表失败: {e}")

    @staticmethod
    async def _create_product_analysis_table():
        """产品分析表"""
        query = f'''
            CREATE TABLE IF NOT EXISTS {settings.DATABASE.CLICKHOUSE_DB}.agg_product_analysis (
                -- 分析日期
                analysis_date Date,
                
                -- 产品信息
                institution_code String,
                product_id String,
                product_code String,
                product_name String,
                product_brand Nullable(String),
                product_category Nullable(String),
                
                -- 销售指标
                sale_count Int64 DEFAULT 0,
                sale_amount Decimal(18, 2) DEFAULT 0,
                customer_count Int64 DEFAULT 0,
                new_customer_count Int64 DEFAULT 0,
                repeat_customer_count Int64 DEFAULT 0,
                
                -- 复购率 = repeat_customer_count / customer_count
                repeat_purchase_rate Float64 DEFAULT 0,
                
                -- 时间戳
                created_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(created_at)
            PARTITION BY toYYYYMM(analysis_date)
            ORDER BY (analysis_date, institution_code, product_id)
        '''
        try:
            await execute_query(query)
            logger.info("✅ 产品分析表创建完成")
        except Exception as e:
            logger.error(f"创建产品分析表失败: {e}")

    @staticmethod
    async def drop_schema():
        """删除Schema"""
        try:
            query = f"DROP DATABASE IF EXISTS {settings.DATABASE.CLICKHOUSE_DB}"
            await execute_query(query)
            logger.info(f"✅ 数据库 {settings.DATABASE.CLICKHOUSE_DB} 已删除")
            return True
        except Exception as e:
            logger.error(f"删除数据库失败: {e}")
            return False

    @staticmethod
    async def get_table_info():
        """获取表信息"""
        query = f'''
            SELECT 
                name,
                engine,
                total_rows,
                total_bytes
            FROM system.tables
            WHERE database = '{settings.DATABASE.CLICKHOUSE_DB}'
            ORDER BY name
        '''
        try:
            result = await execute_query(query)
            return result
        except Exception as e:
            logger.error(f"获取表信息失败: {e}")
            return []


# 方便使用的函数
async def init_clickhouse_schema():
    """初始化ClickHouse Schema"""
    manager = ClickHouseSchemaManager()
    return await manager.init_clickhouse_schema()

