import asyncpg
import logging

from config.constants import *

logger = logging.getLogger(__name__)


class BaseModel:
    """基础模型类"""

    @classmethod
    async def create_table(cls, conn: asyncpg.Connection):
        """创建表"""
        raise NotImplementedError("子类必须实现此方法")

    @classmethod
    async def drop_table(cls, conn: asyncpg.Connection):
        """删除表"""
        table_name = cls.__name__.lower()
        await conn.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")


class NaturalPerson(BaseModel):
    """自然人客户表"""

    @classmethod
    async def create_table(cls, conn: asyncpg.Connection):
        await conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {TABLE_NATURAL_PERSON} (
                person_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                customer_code VARCHAR(20) UNIQUE NOT NULL,
                name VARCHAR(100),
                phone VARCHAR(20) UNIQUE NOT NULL,
                gender VARCHAR(10),
                birthday DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                CONSTRAINT check_gender CHECK (gender IN ('MALE', 'FEMALE', 'UNKNOWN'))
            )
        ''')

        # 创建索引
        await conn.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{TABLE_NATURAL_PERSON}_birthday 
            ON {TABLE_NATURAL_PERSON}(birthday)
        ''')
        await conn.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{TABLE_NATURAL_PERSON}_phone 
            ON {TABLE_NATURAL_PERSON}(phone)
        ''')
        await conn.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{TABLE_NATURAL_PERSON}_created_at 
            ON {TABLE_NATURAL_PERSON}(created_at)
        ''')

    @staticmethod
    async def create_updated_at_trigger(conn: asyncpg.Connection):
        """创建更新时间触发器"""
        await conn.execute('''
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ language 'plpgsql';
        ''')

        await conn.execute(f'''
            DROP TRIGGER IF EXISTS update_{TABLE_NATURAL_PERSON}_updated_at 
            ON {TABLE_NATURAL_PERSON};
        ''')

        await conn.execute(f'''
            CREATE TRIGGER update_{TABLE_NATURAL_PERSON}_updated_at
            BEFORE UPDATE ON {TABLE_NATURAL_PERSON}
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
        ''')


class Institution(BaseModel):
    """医美机构表"""

    @classmethod
    async def create_table(cls, conn: asyncpg.Connection):
        await conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {TABLE_INSTITUTION} (
                institution_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                institution_code VARCHAR(20) UNIQUE NOT NULL,
                name VARCHAR(200) NOT NULL,
                alias VARCHAR(200),
                type VARCHAR(50),
                status VARCHAR(20) DEFAULT 'ACTIVE',
                
                access_password_hash VARCHAR(128),

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        await conn.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{TABLE_INSTITUTION}_institution_code 
            ON {TABLE_INSTITUTION}(institution_code)
        ''')


class InstitutionLoginLog(BaseModel):
    """机构登录日志表"""

    @classmethod
    async def create_table(cls, conn: asyncpg.Connection):
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS institution_login_log (
                log_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                institution_code VARCHAR(20) NOT NULL,
                login_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                success BOOLEAN NOT NULL,
                ip_address VARCHAR(50),
                user_agent TEXT,
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        await conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_institution_login_log_code 
            ON institution_login_log(institution_code)
        ''')

        await conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_institution_login_log_time 
            ON institution_login_log(login_time)
        ''')


class Project(BaseModel):
    """医美项目表"""

    @classmethod
    async def create_table(cls, conn: asyncpg.Connection):
        await conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {TABLE_PROJECT} (
                project_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                project_code VARCHAR(50) UNIQUE NOT NULL,
                name VARCHAR(200) NOT NULL,
                
                category VARCHAR(100),
                body_part VARCHAR(100),
                risk_level INT,
                
                indications TEXT,
                contraindications TEXT,
                description TEXT,

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                CONSTRAINT check_risk CHECK (risk_level >= 1 AND risk_level <= 5)
            )
        ''')

        await conn.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{TABLE_PROJECT}_category 
            ON {TABLE_PROJECT}(category)
        ''')


class Product(BaseModel):
    """医美产品表"""

    @classmethod
    async def create_table(cls, conn: asyncpg.Connection):
        await conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {TABLE_PRODUCT} (
                product_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                product_code VARCHAR(50) UNIQUE NOT NULL,
                name VARCHAR(200) NOT NULL,
                
                brand VARCHAR(100),
                category VARCHAR(100),
                body_part VARCHAR(100),
                unit VARCHAR(50),
                effect_level INT,
                
                indications TEXT,
                contraindications TEXT,
                description TEXT,

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        await conn.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{TABLE_PRODUCT}_brand 
            ON {TABLE_PRODUCT}(brand)
        ''')
        await conn.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{TABLE_PRODUCT}_category 
            ON {TABLE_PRODUCT}(category)
        ''')


class Doctor(BaseModel):
    """医美医生表"""

    @classmethod
    async def create_table(cls, conn: asyncpg.Connection):
        await conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {TABLE_DOCTOR} (
                doctor_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                doctor_code VARCHAR(20) UNIQUE NOT NULL,
                name VARCHAR(100) NOT NULL,
                
                gender VARCHAR(10),
                phone VARCHAR(20),
                
                institution_code VARCHAR(50),
                title VARCHAR(100),
                specialty TEXT[] DEFAULT '{{}}',
                introduction TEXT,
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                CONSTRAINT check_gender CHECK (gender IN ('MALE', 'FEMALE', 'UNKNOWN'))
            )
        ''')

        # 创建索引
        await conn.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{TABLE_DOCTOR}_institution_code 
            ON {TABLE_DOCTOR}(institution_code)
        ''')

        # 对于 TEXT[] 类型的数组字段，使用正确的 GIN 索引语法
        # 注意：这需要 btree_gin 扩展
        try:
            await conn.execute(f'''
                        CREATE INDEX IF NOT EXISTS idx_{TABLE_DOCTOR}_specialty 
                        ON {TABLE_DOCTOR} USING gin(specialty)
                    ''')
            logger.info("✅ Doctor 表 GIN 索引创建完成")
        except Exception as e:
            logger.warning(f"⚠️ 创建 specialty GIN 索引失败: {e}")
            logger.warning("⚠️ 这可能是因为缺少 btree_gin 扩展，索引已跳过")


class MedicalRelation(BaseModel):
    """医美项目/产品关系表（集团公开表）"""

    @classmethod
    async def create_table(cls, conn: asyncpg.Connection):
        await conn.execute(f'''
            CREATE TABLE IF NOT EXISTS medical_relation (
                relation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

                source_type VARCHAR(20) NOT NULL,
                source_id UUID NOT NULL,
                target_type VARCHAR(20) NOT NULL,
                target_id UUID NOT NULL,

                relation_type VARCHAR(50) NOT NULL,
                relation_level INT DEFAULT 1,
                is_bidirectional BOOLEAN DEFAULT FALSE,
                description TEXT,

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                CONSTRAINT check_source_type CHECK (source_type IN ('PROJECT', 'PRODUCT')),
                CONSTRAINT check_target_type CHECK (target_type IN ('PROJECT', 'PRODUCT')),
                CONSTRAINT check_relation_type CHECK (relation_type IN ('UPGRADE', 'SIMILAR', 'COMBINATION', 'PREREQUISITE', 'ALTERNATIVE')),
                CONSTRAINT check_relation_level CHECK (relation_level >= 1 AND relation_level <= 5),
                CONSTRAINT check_not_self_relation CHECK (source_type != target_type OR source_id != target_id)
            )
        ''')

        # 创建复合唯一约束
        try:
            await conn.execute(f'''
                ALTER TABLE medical_relation 
                ADD CONSTRAINT unique_medical_relation 
                UNIQUE (source_type, source_id, target_type, target_id, relation_type)
            ''')
        except asyncpg.exceptions.DuplicateObjectError:
            pass

        # 创建索引
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_medical_relation_source ON medical_relation(source_type, source_id)",
            "CREATE INDEX IF NOT EXISTS idx_medical_relation_target ON medical_relation(target_type, target_id)",
            "CREATE INDEX IF NOT EXISTS idx_medical_relation_type ON medical_relation(relation_type)",
            "CREATE INDEX IF NOT EXISTS idx_medical_relation_bidirectional ON medical_relation(is_bidirectional)"
        ]

        for index_sql in indexes:
            try:
                await conn.execute(index_sql)
            except Exception as e:
                logger.warning(f"创建索引失败: {e}")


class InstitutionTableCreator:
    """机构特定表创建器"""

    @staticmethod
    def get_table_suffix(institution_code: str) -> str:
        """获取表名后缀"""
        return institution_code.lower().replace("-", "_")

    @staticmethod
    def get_table_name(base_name: str, institution_code: str) -> str:
        """获取完整表名"""
        suffix = InstitutionTableCreator.get_table_suffix(institution_code)
        return f"{base_name}_{suffix}"

    @classmethod
    async def create_tables_for_institution(
            cls,
            conn: asyncpg.Connection,
            institution_code: str
    ):
        """为机构创建所有特定表"""
        suffix = cls.get_table_suffix(institution_code)

        # 1. 创建机构客户表
        await cls._create_institution_customer_table(conn, suffix)

        # 2. 创建机构配置表（项目定价表、产品库存表、医生执业表）
        await cls._create_institution_config_tables(conn, suffix)

        # 3. 创建业务表
        await cls._create_institution_business_tables(conn, suffix)

        # 4. 创建回访相关表
        await cls._create_institution_followup_tables(conn, suffix)

        # 5. 创建分析表
        await cls._create_institution_analysis_tables(conn, suffix)

    @staticmethod
    async def _create_institution_customer_table(conn: asyncpg.Connection, suffix: str):
        """创建机构客户表"""
        table_name = f"institution_customer_{suffix}"

        await conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                institution_customer_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                institution_id UUID NOT NULL REFERENCES {TABLE_INSTITUTION}(institution_id),
                person_id UUID NOT NULL REFERENCES {TABLE_NATURAL_PERSON}(person_id),
                
                customer_code VARCHAR(100) UNIQUE NOT NULL,
                vip_level VARCHAR(50) DEFAULT '{CUSTOMER_LEVEL_NORMAL}',
                status VARCHAR(20) DEFAULT 'ACTIVE',
                
                first_visit_date DATE,
                last_visit_date DATE,
                consumption_count INT DEFAULT 0,
                total_consumption DECIMAL(12,2) DEFAULT 0,
                
                referrer_id UUID,
                doctor_id UUID REFERENCES {TABLE_DOCTOR}(doctor_id),
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                UNIQUE(institution_id, person_id),
                UNIQUE(institution_id, customer_code),
                
                CONSTRAINT check_status 
                    CHECK (status IN ('ACTIVE', 'DORMANT', 'CHURNED', 'SUSPENDED'))
            )
        ''')

        # 创建索引
        indexes = [
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_person_id ON {table_name}(person_id)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_total_consumption ON {table_name}(total_consumption)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_vip_level ON {table_name}(vip_level)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_last_visit_date ON {table_name}(last_visit_date)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_created_at ON {table_name}(created_at)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_customer_code ON {table_name}(customer_code)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_referrer_id ON {table_name}(referrer_id)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_status ON {table_name}(status)"
        ]

        for index_sql in indexes:
            await conn.execute(index_sql)

    @staticmethod
    async def _create_institution_config_tables(conn: asyncpg.Connection, suffix: str):
        """创建机构配置表"""
        # 1. 机构项目定价表
        table_name = f"institution_project_{suffix}"
        await conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                institution_project_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                institution_id UUID NOT NULL REFERENCES {TABLE_INSTITUTION}(institution_id),
                project_id UUID NOT NULL REFERENCES {TABLE_PROJECT}(project_id),

                price DECIMAL(10,2) NOT NULL,
                is_available BOOLEAN DEFAULT TRUE,
                available_from DATE,
                available_to DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                UNIQUE(institution_id, project_id)
            )
        ''')

        await conn.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{table_name}_is_available 
            ON {table_name}(is_available)
        ''')

        # 2. 机构产品库存表
        table_name = f"institution_product_{suffix}"
        await conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                institution_product_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                institution_id UUID NOT NULL REFERENCES {TABLE_INSTITUTION}(institution_id),
                product_id UUID NOT NULL REFERENCES {TABLE_PRODUCT}(product_id),

                stock INT DEFAULT 0,
                price DECIMAL(10,2) NOT NULL,
                is_available BOOLEAN DEFAULT TRUE,
                available_from DATE,
                available_to DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                UNIQUE(institution_id, product_id)
            )
        ''')

        await conn.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{table_name}_is_available 
            ON {table_name}(is_available)
        ''')

        # 3. 医生机构执业表
        table_name = f"institution_doctor_{suffix}"
        await conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                institution_doctor_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                institution_id UUID NOT NULL REFERENCES {TABLE_INSTITUTION}(institution_id),
                doctor_id UUID NOT NULL REFERENCES {TABLE_DOCTOR}(doctor_id),

                department VARCHAR(100),
                status VARCHAR(20) DEFAULT 'ACTIVE',
                start_date DATE NOT NULL,
                end_date DATE,

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                UNIQUE(institution_id, doctor_id)
            )
        ''')

        await conn.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{table_name}_department 
            ON {table_name}(department)
        ''')
        await conn.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{table_name}_status 
            ON {table_name}(status)
        ''')

    @staticmethod
    async def _create_institution_business_tables(conn: asyncpg.Connection, suffix: str):
        """创建机构业务表"""
        # 1. 线上咨询记录表
        table_name = f"online_consultation_{suffix}"
        await conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                online_consultation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                institution_id UUID NOT NULL REFERENCES {TABLE_INSTITUTION}(institution_id),
                institution_customer_id UUID NOT NULL REFERENCES institution_customer_{suffix}(institution_customer_id),

                channel VARCHAR(20) NOT NULL,
                consultation_date DATE NOT NULL,
                main_concern TEXT,

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        await conn.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{table_name}_institution_customer_id 
            ON {table_name}(institution_customer_id)
        ''')
        await conn.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{table_name}_consultation_date 
            ON {table_name}(consultation_date)
        ''')

        # 2. 线下到院咨询记录表
        table_name = f"offline_consultation_{suffix}"
        await conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                offline_consultation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                institution_id UUID NOT NULL REFERENCES {TABLE_INSTITUTION}(institution_id),
                institution_customer_id UUID NOT NULL REFERENCES institution_customer_{suffix}(institution_customer_id),

                consultation_date DATE NOT NULL,
                consultation_type VARCHAR(20),
                main_concern TEXT,

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # 3. 到院消费记录表
        table_name = f"consumption_record_{suffix}"
        await conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                consumption_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                
                institution_id UUID NOT NULL REFERENCES {TABLE_INSTITUTION}(institution_id),
                institution_customer_id UUID NOT NULL REFERENCES institution_customer_{suffix}(institution_customer_id),
                institution_project_id UUID REFERENCES institution_project_{suffix}(institution_project_id),
                institution_product_id UUID REFERENCES institution_product_{suffix}(institution_product_id),
                institution_doctor_id UUID REFERENCES institution_doctor_{suffix}(institution_doctor_id),

                order_number VARCHAR(50) UNIQUE NOT NULL,
                order_date DATE NOT NULL,
                order_time TIME,
                order_type VARCHAR(20),

                current_times INT DEFAULT 1,
                total_times INT DEFAULT 1,

                total_amount DECIMAL(12,2) NOT NULL,
                discount_amount DECIMAL(12,2) DEFAULT 0,
                actual_amount DECIMAL(12,2) NOT NULL,
                payment_method VARCHAR(20),
                payment_status VARCHAR(20) DEFAULT 'PAID',
                
                is_refund BOOLEAN DEFAULT FALSE,
                refund_amount DECIMAL(12,2),
                refund_reason TEXT,
                
                notes TEXT,

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                CONSTRAINT check_total_amount CHECK (total_amount >= 0),
                CONSTRAINT check_actual_amount CHECK (actual_amount >= 0)
            )
        ''')

        await conn.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{table_name}_institution_customer_id 
            ON {table_name}(institution_customer_id)
        ''')
        await conn.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{table_name}_order_date 
            ON {table_name}(order_date)
        ''')

    @staticmethod
    async def _create_institution_followup_tables(conn: asyncpg.Connection, suffix: str):
        """创建机构回访相关表"""
        # 1. 机构生日待回访列表
        table_name = f"birthday_reminder_{suffix}"
        await conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                birthday_reminder_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                institution_id UUID NOT NULL REFERENCES {TABLE_INSTITUTION}(institution_id),
                institution_customer_id UUID NOT NULL REFERENCES institution_customer_{suffix}(institution_customer_id),

                birth_month INT NOT NULL,
                birth_day INT NOT NULL,
                
                reminder_type VARCHAR(20),
                reminder_date DATE NOT NULL,
                reminder_status VARCHAR(20) DEFAULT 'PENDING',
                complete_date DATE,
                
                notes TEXT,

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                UNIQUE(institution_id, institution_customer_id, reminder_date),

                CONSTRAINT check_birth_month CHECK (birth_month >= 1 AND birth_month <= 12),
                CONSTRAINT check_birth_day CHECK (birth_day >= 1 AND birth_day <= 31),
                CONSTRAINT check_reminder_status CHECK (reminder_status IN ('PENDING', 'DEFERRED', 'COMPLETED'))
            )
        ''')

        await conn.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{table_name}_reminder_date 
            ON {table_name}(reminder_date)
        ''')

        # 2. 机构完整回访列表
        table_name = f"reminder_record_{suffix}"
        await conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                reminder_record_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                institution_id UUID NOT NULL REFERENCES {TABLE_INSTITUTION}(institution_id),
                institution_customer_id UUID NOT NULL REFERENCES institution_customer_{suffix}(institution_customer_id),

                reminder_type VARCHAR(30) NOT NULL,
                reminder_date DATE,
                reminder_status VARCHAR(20) DEFAULT 'COMPLETED',
                complete_date DATE,

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        await conn.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{table_name}_institution_customer_id 
            ON {table_name}(institution_customer_id)
        ''')
        await conn.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{table_name}_reminder_date 
            ON {table_name}(reminder_date)
        ''')

        # 3. 聊天会话记录表
        # messages JSONB 格式示例:
        # [
        #   {"sender_type": "SYSTEM", "content": "咨询师已接入", "message_type": "NOTIFICATION", "time": "2026-01-14T10:00:00"},
        #   {"sender_type": "CUSTOMER", "content": "你好，我想咨询一下", "message_type": "TEXT", "time": "2026-01-14T10:00:30"},
        #   {"sender_type": "INSTITUTION", "content": "您好，请问有什么可以帮您？", "message_type": "TEXT", "time": "2026-01-14T10:01:00"}
        # ]
        # sender_type: CUSTOMER(客户) / INSTITUTION(机构/咨询师) / SYSTEM(系统)
        table_name = f"chat_session_{suffix}"
        await conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                institution_id UUID NOT NULL REFERENCES {TABLE_INSTITUTION}(institution_id),
                institution_customer_id UUID REFERENCES institution_customer_{suffix}(institution_customer_id),
                online_consultation_id UUID REFERENCES online_consultation_{suffix}(online_consultation_id),

                session_start_time TIMESTAMP NOT NULL,
                session_end_time TIMESTAMP,
                session_duration_minutes INT DEFAULT 0,

                messages JSONB NOT NULL DEFAULT '[]'::jsonb,

                total_message_count INT DEFAULT 0,
                customer_message_count INT DEFAULT 0,
                institution_message_count INT DEFAULT 0,
                system_message_count INT DEFAULT 0,

                first_response_seconds INT,
                avg_response_seconds INT,

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        await conn.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{table_name}_online_consultation_id 
            ON {table_name}(online_consultation_id)
        ''')
        await conn.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{table_name}_institution_customer_id 
            ON {table_name}(institution_customer_id)
        ''')
        await conn.execute(f'''
            CREATE INDEX IF NOT EXISTS idx_{table_name}_session_start_time 
            ON {table_name}(session_start_time)
        ''')

    @staticmethod
    async def _create_institution_analysis_tables(conn: asyncpg.Connection, suffix: str):
        """创建机构分析表"""
        # 1. 用户人格表
        table_name = f"customer_personality_{suffix}"
        await conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                personality_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                institution_customer_id UUID NOT NULL REFERENCES institution_customer_{suffix}(institution_customer_id) UNIQUE,

                personality_summary TEXT,
                last_analysis_date DATE,
                confidence_score DECIMAL(3,2),
                version INT DEFAULT 1,

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                CONSTRAINT check_confidence_score CHECK (confidence_score >= 0 AND confidence_score <= 1)
            )
        ''')

        # 2. 用户昵称表
        table_name = f"customer_nickname_{suffix}"
        await conn.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                nickname_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                institution_customer_id UUID NOT NULL REFERENCES institution_customer_{suffix}(institution_customer_id),

                nickname VARCHAR(50) NOT NULL,
                nickname_type VARCHAR(20),

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                UNIQUE(institution_customer_id, nickname),

                CONSTRAINT check_nickname_type CHECK (nickname_type IN ('STAFF_GIVEN', 'CLIENT_PREFERRED'))
            )
        ''')

    @staticmethod
    async def add_referrer_foreign_key(conn: asyncpg.Connection, suffix: str):
        """为推荐人字段添加外键约束（在表创建后调用）"""
        table_name = f"institution_customer_{suffix}"

        try:
            await conn.execute(f'''
                ALTER TABLE {table_name} 
                ADD CONSTRAINT fk_{table_name}_referrer 
                FOREIGN KEY (referrer_id) 
                REFERENCES {table_name}(institution_customer_id)
                ON DELETE SET NULL
            ''')
            logger.info(f"✅ 已为 {table_name} 添加推荐人外键约束")
        except Exception as e:
            logger.warning(f"⚠️ 添加推荐人外键约束失败: {e}")


class DatabaseInitializer:
    """数据库初始化器"""

    @staticmethod
    async def initialize_database(conn: asyncpg.Connection):
        """初始化数据库，创建所有表"""
        logger.info("开始初始化数据库...")

        # 创建扩展
        await conn.execute('''
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
            CREATE EXTENSION IF NOT EXISTS "pgcrypto";
            CREATE EXTENSION IF NOT EXISTS "btree_gin";  -- 添加 btree_gin 扩展以支持更多类型的 GIN 索引
        ''')

        # 创建集团公开表
        await NaturalPerson.create_table(conn)
        await NaturalPerson.create_updated_at_trigger(conn)
        logger.info("✅ NaturalPerson 表创建完成")

        await Institution.create_table(conn)
        logger.info("✅ Institution 表创建完成")

        await InstitutionLoginLog.create_table(conn)
        logger.info("✅ InstitutionLoginLog 表创建完成")

        await Project.create_table(conn)
        logger.info("✅ Project 表创建完成")

        await Product.create_table(conn)
        logger.info("✅ Product 表创建完成")

        await Doctor.create_table(conn)
        logger.info("✅ Doctor 表创建完成")

        # 新增：创建医美关系表
        await MedicalRelation.create_table(conn)
        logger.info("✅ MedicalRelation 表创建完成")

        logger.info("✅ 所有集团公开表创建完成")

    @staticmethod
    async def create_institution_tables(
            conn: asyncpg.Connection,
            institution_code: str
    ):
        """为特定机构创建表"""
        logger.info(f"为机构 {institution_code} 创建表...")
        try:
            await InstitutionTableCreator.create_tables_for_institution(
                conn, institution_code
            )

            # 在所有表创建完成后，添加推荐人字段的外键约束
            suffix = InstitutionTableCreator.get_table_suffix(institution_code)
            await InstitutionTableCreator.add_referrer_foreign_key(conn, suffix)

            logger.info(f"✅ 机构 {institution_code} 的所有表创建完成")
            return True
        except Exception as e:
            logger.error(f"❌ 创建机构 {institution_code} 表时出错: {e}")
            return False