"""
数据同步服务
负责将数据从 PostgreSQL 主库同步到其他数据库（NebulaGraph、Qdrant、ClickHouse）

数据流向：
  JSON 文件 → PostgreSQL (主数据) → NebulaGraph (图关系)
                                   → Qdrant (向量搜索)
                                   → ClickHouse (分析统计)
"""
import logging
from typing import Dict, Optional
from datetime import datetime

from database.postgres.connection import PostgreSQLConnection
from database.postgres.models import DatabaseInitializer
from database.nebula.connection import NebulaConnection, execute_ngql
from database.qdrant.connection import QdrantConnection
from database.clickhouse.connection import execute_query as ch_execute_query
from config.settings import settings

logger = logging.getLogger(__name__)

# 缓存已确认存在的机构表
_institution_tables_cache: set = set()


class DataSyncService:
    """数据同步服务"""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self.pg = PostgreSQLConnection
        self.nebula = NebulaConnection
        self._qdrant_client = None
        self._initialized = True

    async def init_connections(self):
        """初始化所有数据库连接"""
        logger.info("正在初始化数据库连接...")
        try:
            await self.pg.create_pool()
            logger.info("  ✅ PostgreSQL 连接池已创建")
        except Exception as e:
            logger.error(f"  ❌ PostgreSQL 连接失败: {e}")

        try:
            self.nebula.init_connection()
            logger.info("  ✅ NebulaGraph 连接已建立")
        except Exception as e:
            logger.warning(f"  ⚠️ NebulaGraph 连接失败: {e}")

        try:
            qdrant = QdrantConnection()
            self._qdrant_client = qdrant.get_client()
            logger.info("  ✅ Qdrant 连接已建立")
        except Exception as e:
            logger.warning(f"  ⚠️ Qdrant 连接失败: {e}")

        logger.info("✅ 数据库连接初始化完成")

    async def close_connections(self):
        """关闭所有数据库连接"""
        try:
            await self.pg.close_pool()
        except Exception as e:
            logger.warning(f"关闭 PostgreSQL 连接时出错: {e}")

        try:
            self.nebula.close_session()
            self.nebula.close_connection_pool()
        except Exception as e:
            logger.warning(f"关闭 NebulaGraph 连接时出错: {e}")

        logger.info("✅ 所有数据库连接已关闭")

    # ==================== PostgreSQL 操作 ====================

    async def ensure_institution_tables(self, institution_code: str) -> bool:
        """
        确保指定机构的所有相关表存在

        在导入数据前调用此方法，动态创建缺失的机构表。
        使用缓存避免重复检查。

        Args:
            institution_code: 机构编码，如 'BJ-HA-001'

        Returns:
            bool: 表是否存在（或成功创建）
        """
        global _institution_tables_cache

        # 如果已在缓存中，直接返回
        if institution_code in _institution_tables_cache:
            return True

        suffix = institution_code.lower().replace('-', '_')
        customer_table = f"institution_customer_{suffix}"

        async with self.pg.get_connection() as conn:
            # 检查机构客户表是否存在
            exists = await conn.fetchval('''
                SELECT EXISTS (
                    SELECT FROM pg_tables 
                    WHERE schemaname = 'public' 
                    AND tablename = $1
                )
            ''', customer_table)

            if not exists:
                logger.info(f"🏥 正在为机构 {institution_code} 创建表...")
                try:
                    # 首先确保机构记录存在
                    await conn.execute('''
                        INSERT INTO institution (institution_code, name, status)
                        VALUES ($1, $2, 'ACTIVE')
                        ON CONFLICT (institution_code) DO NOTHING
                    ''', institution_code, f'机构 {institution_code}')

                    # 创建机构特定表
                    success = await DatabaseInitializer.create_institution_tables(
                        conn, institution_code
                    )

                    if success:
                        logger.info(f"✅ 机构 {institution_code} 的表创建成功")
                        _institution_tables_cache.add(institution_code)
                        return True
                    else:
                        logger.error(f"❌ 机构 {institution_code} 的表创建失败")
                        return False

                except Exception as e:
                    logger.error(f"❌ 创建机构 {institution_code} 表时出错: {e}")
                    return False
            else:
                # 表已存在，添加到缓存
                _institution_tables_cache.add(institution_code)
                return True

    async def upsert_institution(self, data: Dict) -> Optional[str]:
        """插入或更新机构"""
        async with self.pg.get_connection() as conn:
            result = await conn.fetchrow('''
                INSERT INTO institution (institution_code, name, alias, type, status)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (institution_code) DO UPDATE SET
                    name = EXCLUDED.name,
                    alias = EXCLUDED.alias,
                    type = EXCLUDED.type,
                    status = EXCLUDED.status,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING institution_id::text
            ''', data['institution_code'], data['name'],
                data.get('alias'), data.get('type'), data.get('status', 'ACTIVE'))
            return result['institution_id'] if result else None

    async def upsert_doctor(self, data: Dict) -> Optional[str]:
        """插入或更新医生"""
        async with self.pg.get_connection() as conn:
            specialty = data.get('specialty', [])
            if isinstance(specialty, str):
                specialty = [specialty]

            result = await conn.fetchrow('''
                INSERT INTO doctor (doctor_code, name, gender, phone, institution_code, title, specialty, introduction)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (doctor_code) DO UPDATE SET
                    name = EXCLUDED.name,
                    gender = EXCLUDED.gender,
                    phone = EXCLUDED.phone,
                    institution_code = EXCLUDED.institution_code,
                    title = EXCLUDED.title,
                    specialty = EXCLUDED.specialty,
                    introduction = EXCLUDED.introduction,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING doctor_id::text
            ''', data['doctor_code'], data['name'], data.get('gender'),
                data.get('phone'), data.get('institution_code'),
                data.get('title'), specialty, data.get('introduction'))

            # 如果指定了机构，创建机构医生关联
            if result and data.get('institution_code'):
                await self._create_institution_doctor_relation(
                    data['doctor_code'], data['institution_code'], result['doctor_id']
                )

            return result['doctor_id'] if result else None

    async def _create_institution_doctor_relation(self, doctor_code: str, institution_code: str, doctor_id: str):
        """创建机构-医生关联"""
        try:
            # 确保机构表存在
            if not await self.ensure_institution_tables(institution_code):
                return

            suffix = institution_code.lower().replace('-', '_')
            table_name = f"institution_doctor_{suffix}"

            async with self.pg.get_connection() as conn:
                institution = await conn.fetchrow(
                    'SELECT institution_id FROM institution WHERE institution_code = $1',
                    institution_code
                )
                if institution:
                    await conn.execute(f'''
                        INSERT INTO {table_name} (institution_id, doctor_id, status, start_date)
                        VALUES ($1, $2, 'ACTIVE', CURRENT_DATE)
                        ON CONFLICT (institution_id, doctor_id) DO NOTHING
                    ''', institution['institution_id'], doctor_id)
        except Exception as e:
            logger.debug(f"创建机构医生关联失败: {e}")

    async def upsert_project(self, data: Dict) -> Optional[str]:
        """插入或更新项目"""
        async with self.pg.get_connection() as conn:
            result = await conn.fetchrow('''
                INSERT INTO project (project_code, name, category, body_part, risk_level, indications, contraindications, description)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (project_code) DO UPDATE SET
                    name = EXCLUDED.name,
                    category = EXCLUDED.category,
                    body_part = EXCLUDED.body_part,
                    risk_level = EXCLUDED.risk_level,
                    indications = EXCLUDED.indications,
                    contraindications = EXCLUDED.contraindications,
                    description = EXCLUDED.description,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING project_id::text
            ''', data['project_code'], data['name'], data.get('category'),
                data.get('body_part'), data.get('risk_level'),
                data.get('indications'), data.get('contraindications'),
                data.get('description'))

            # 为所有机构创建关联
            if result:
                await self._create_institution_project_relations(
                    data['project_code'], result['project_id'], data.get('price')
                )

            return result['project_id'] if result else None

    async def _create_institution_project_relations(self, project_code: str, project_id: str, price=None):
        """为所有机构创建项目关联"""
        from config.settings import settings
        for institution_code in settings.APP.INSTITUTIONS:
            try:
                if not await self.ensure_institution_tables(institution_code):
                    continue

                suffix = institution_code.lower().replace('-', '_')
                table_name = f"institution_project_{suffix}"

                async with self.pg.get_connection() as conn:
                    institution = await conn.fetchrow(
                        'SELECT institution_id FROM institution WHERE institution_code = $1',
                        institution_code
                    )
                    if institution:
                        await conn.execute(f'''
                            INSERT INTO {table_name} (institution_id, project_id, price, is_available)
                            VALUES ($1, $2, $3, true)
                            ON CONFLICT (institution_id, project_id) DO NOTHING
                        ''', institution['institution_id'], project_id, price or 0)
            except Exception as e:
                logger.debug(f"创建机构项目关联失败 {institution_code}: {e}")

    async def upsert_product(self, data: Dict) -> Optional[str]:
        """插入或更新产品"""
        async with self.pg.get_connection() as conn:
            result = await conn.fetchrow('''
                INSERT INTO product (product_code, name, brand, category, body_part, unit, effect_level, indications, contraindications, description)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (product_code) DO UPDATE SET
                    name = EXCLUDED.name,
                    brand = EXCLUDED.brand,
                    category = EXCLUDED.category,
                    body_part = EXCLUDED.body_part,
                    unit = EXCLUDED.unit,
                    effect_level = EXCLUDED.effect_level,
                    indications = EXCLUDED.indications,
                    contraindications = EXCLUDED.contraindications,
                    description = EXCLUDED.description,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING product_id::text
            ''', data['product_code'], data['name'], data.get('brand'),
                data.get('category'), data.get('body_part'), data.get('unit'),
                data.get('effect_level'), data.get('indications'),
                data.get('contraindications'), data.get('description'))

            # 为所有机构创建关联
            if result:
                await self._create_institution_product_relations(
                    data['product_code'], result['product_id'], data.get('price')
                )

            return result['product_id'] if result else None

    async def _create_institution_product_relations(self, product_code: str, product_id: str, price=None):
        """为所有机构创建产品关联"""
        from config.settings import settings
        for institution_code in settings.APP.INSTITUTIONS:
            try:
                if not await self.ensure_institution_tables(institution_code):
                    continue

                suffix = institution_code.lower().replace('-', '_')
                table_name = f"institution_product_{suffix}"

                async with self.pg.get_connection() as conn:
                    institution = await conn.fetchrow(
                        'SELECT institution_id FROM institution WHERE institution_code = $1',
                        institution_code
                    )
                    if institution:
                        await conn.execute(f'''
                            INSERT INTO {table_name} (institution_id, product_id, price, is_available)
                            VALUES ($1, $2, $3, true)
                            ON CONFLICT (institution_id, product_id) DO NOTHING
                        ''', institution['institution_id'], product_id, price or 0)
            except Exception as e:
                logger.debug(f"创建机构产品关联失败 {institution_code}: {e}")

    async def upsert_customer(self, data: Dict, institution_code: str) -> Dict[str, str]:
        """插入或更新客户（包括自然人和机构客户）"""
        # 确保机构表存在
        if not await self.ensure_institution_tables(institution_code):
            raise ValueError(f"无法创建机构 {institution_code} 的表")

        async with self.pg.get_connection() as conn:
            # 1. 先处理自然人
            person_data = data.get('person', {})
            birthday = person_data.get('birthday')
            if birthday and isinstance(birthday, str):
                birthday = datetime.strptime(birthday, '%Y-%m-%d').date()

            # 生成客户编码（用于自然人表）
            customer_code = data['customer_code']

            person_result = await conn.fetchrow('''
                INSERT INTO natural_person (customer_code, name, phone, gender, birthday)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (phone) DO UPDATE SET
                    name = COALESCE(EXCLUDED.name, natural_person.name),
                    gender = COALESCE(EXCLUDED.gender, natural_person.gender),
                    birthday = COALESCE(EXCLUDED.birthday, natural_person.birthday),
                    updated_at = CURRENT_TIMESTAMP
                RETURNING person_id::text
            ''', customer_code, person_data.get('name'), person_data['phone'],
                person_data.get('gender'), birthday)

            person_id = person_result['person_id']

            # 2. 获取机构ID
            institution = await conn.fetchrow(
                'SELECT institution_id FROM institution WHERE institution_code = $1',
                institution_code
            )
            if not institution:
                raise ValueError(f"机构不存在: {institution_code}")
            institution_id = institution['institution_id']

            # 3. 获取医生ID（如果有）
            doctor_id = None
            if data.get('doctor_code'):
                doctor = await conn.fetchrow(
                    'SELECT doctor_id FROM doctor WHERE doctor_code = $1',
                    data['doctor_code']
                )
                if doctor:
                    doctor_id = doctor['doctor_id']

            # 4. 处理日期
            first_visit = data.get('first_visit_date')
            if first_visit and isinstance(first_visit, str):
                first_visit = datetime.strptime(first_visit, '%Y-%m-%d').date()

            last_visit = data.get('last_visit_date')
            if last_visit and isinstance(last_visit, str):
                last_visit = datetime.strptime(last_visit, '%Y-%m-%d').date()

            # 5. 插入机构客户表
            suffix = institution_code.lower().replace('-', '_')
            table_name = f"institution_customer_{suffix}"

            # 先查询是否存在推荐人
            referrer_id = None
            if data.get('referrer_code'):
                referrer = await conn.fetchrow(
                    f'SELECT institution_customer_id FROM {table_name} WHERE customer_code = $1',
                    data['referrer_code']
                )
                if referrer:
                    referrer_id = referrer['institution_customer_id']

            inst_customer_result = await conn.fetchrow(f'''
                INSERT INTO {table_name} 
                    (institution_id, person_id, customer_code, vip_level, status, 
                     first_visit_date, last_visit_date, referrer_id, doctor_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (institution_id, person_id) DO UPDATE SET
                    vip_level = EXCLUDED.vip_level,
                    status = EXCLUDED.status,
                    first_visit_date = COALESCE({table_name}.first_visit_date, EXCLUDED.first_visit_date),
                    last_visit_date = COALESCE(EXCLUDED.last_visit_date, {table_name}.last_visit_date),
                    referrer_id = COALESCE(EXCLUDED.referrer_id, {table_name}.referrer_id),
                    doctor_id = COALESCE(EXCLUDED.doctor_id, {table_name}.doctor_id),
                    updated_at = CURRENT_TIMESTAMP
                RETURNING institution_customer_id::text
            ''', institution_id, person_id, customer_code,
                data.get('vip_level', 'NORMAL'), data.get('status', 'ACTIVE'),
                first_visit, last_visit, referrer_id, doctor_id)

            return {
                'person_id': person_id,
                'institution_customer_id': inst_customer_result['institution_customer_id'],
                'institution_id': str(institution_id)
            }

    async def insert_consumption_record(self, data: Dict, institution_code: str) -> Optional[str]:
        """插入消费记录"""
        # 确保机构表存在
        if not await self.ensure_institution_tables(institution_code):
            raise ValueError(f"无法创建机构 {institution_code} 的表")

        async with self.pg.get_connection() as conn:
            # 获取机构ID
            institution = await conn.fetchrow(
                'SELECT institution_id FROM institution WHERE institution_code = $1',
                institution_code
            )
            if not institution:
                raise ValueError(f"机构不存在: {institution_code}")

            # 获取客户ID
            suffix = institution_code.lower().replace('-', '_')
            customer_table = f"institution_customer_{suffix}"
            customer = await conn.fetchrow(
                f'SELECT institution_customer_id FROM {customer_table} WHERE customer_code = $1',
                data['customer_code']
            )
            if not customer:
                logger.warning(f"客户不存在: {data['customer_code']}")
                return None
            customer_id = customer['institution_customer_id']

            # 获取项目关联ID（如果有）
            inst_project_id = None
            if data.get('project_code'):
                inst_project_table = f"institution_project_{suffix}"
                project_row = await conn.fetchrow(f'''
                    SELECT ip.institution_project_id 
                    FROM {inst_project_table} ip
                    JOIN project p ON ip.project_id = p.project_id
                    WHERE p.project_code = $1 AND ip.institution_id = $2
                ''', data['project_code'], institution['institution_id'])
                if project_row:
                    inst_project_id = project_row['institution_project_id']
                else:
                    logger.debug(f"项目关联不存在: {data['project_code']}")

            # 获取产品关联ID（如果有）
            inst_product_id = None
            if data.get('product_code'):
                inst_product_table = f"institution_product_{suffix}"
                product_row = await conn.fetchrow(f'''
                    SELECT ipr.institution_product_id 
                    FROM {inst_product_table} ipr
                    JOIN product pr ON ipr.product_id = pr.product_id
                    WHERE pr.product_code = $1 AND ipr.institution_id = $2
                ''', data['product_code'], institution['institution_id'])
                if product_row:
                    inst_product_id = product_row['institution_product_id']
                else:
                    logger.debug(f"产品关联不存在: {data['product_code']}")

            # 获取医生关联ID（如果有）
            inst_doctor_id = None
            if data.get('doctor_code'):
                inst_doctor_table = f"institution_doctor_{suffix}"
                doctor_row = await conn.fetchrow(f'''
                    SELECT id.institution_doctor_id 
                    FROM {inst_doctor_table} id
                    JOIN doctor d ON id.doctor_id = d.doctor_id
                    WHERE d.doctor_code = $1 AND id.institution_id = $2
                ''', data['doctor_code'], institution['institution_id'])
                if doctor_row:
                    inst_doctor_id = doctor_row['institution_doctor_id']
                else:
                    logger.debug(f"医生关联不存在: {data['doctor_code']}")

            # 处理日期
            order_date = data.get('order_date')
            if order_date and isinstance(order_date, str):
                order_date = datetime.strptime(order_date, '%Y-%m-%d').date()

            # 处理时间
            order_time = data.get('order_time')
            if order_time and isinstance(order_time, str):
                order_time = datetime.strptime(order_time, '%H:%M:%S').time()

            # 插入消费记录
            order_table = f"consumption_record_{suffix}"
            result = await conn.fetchrow(f'''
                INSERT INTO {order_table} 
                    (order_number, institution_id, institution_customer_id, 
                     institution_project_id, institution_product_id, institution_doctor_id,
                     order_date, order_time, order_type,
                     current_times, total_times,
                     total_amount, discount_amount, actual_amount, 
                     payment_method, payment_status, is_refund, notes)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
                ON CONFLICT (order_number) DO UPDATE SET
                    institution_project_id = COALESCE(EXCLUDED.institution_project_id, {order_table}.institution_project_id),
                    institution_product_id = COALESCE(EXCLUDED.institution_product_id, {order_table}.institution_product_id),
                    institution_doctor_id = COALESCE(EXCLUDED.institution_doctor_id, {order_table}.institution_doctor_id),
                    updated_at = CURRENT_TIMESTAMP
                RETURNING consumption_id::text
            ''', data['order_number'], institution['institution_id'], customer_id,
                inst_project_id, inst_product_id, inst_doctor_id,
                order_date, order_time, data.get('order_type'),
                data.get('current_times', 1), data.get('total_times', 1),
                data.get('total_amount', 0), data.get('discount_amount', 0),
                data.get('actual_amount', 0), data.get('payment_method'),
                data.get('payment_status', 'PAID'), data.get('is_refund', False),
                data.get('notes'))

            # 更新客户消费统计
            if result:
                await conn.execute(f'''
                    UPDATE {customer_table} SET
                        consumption_count = consumption_count + 1,
                        total_consumption = total_consumption + $1,
                        last_visit_date = $2,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE institution_customer_id = $3
                ''', data.get('actual_amount', 0), order_date, customer_id)

            return result['consumption_id'] if result else None

    # ==================== 查询方法 ====================

    async def get_upcoming_birthday_customers(self, institution_code: str, days_ahead: int = 7):
        """获取即将生日的客户"""
        from datetime import date, timedelta
        suffix = institution_code.lower().replace('-', '_')
        table_name = f"institution_customer_{suffix}"
        today = date.today()

        async with self.pg.get_connection() as conn:
            # 构建日期范围查询
            customers = []
            for i in range(days_ahead + 1):
                target_date = today + timedelta(days=i)
                rows = await conn.fetch(f'''
                    SELECT 
                        ic.institution_customer_id,
                        ic.customer_code,
                        ic.vip_level,
                        ic.status,
                        ic.first_visit_date,
                        ic.last_visit_date,
                        ic.consumption_count,
                        ic.total_consumption,
                        np.person_id,
                        np.name,
                        np.phone,
                        np.gender,
                        np.birthday,
                        {i} as days_until_birthday
                    FROM {table_name} ic
                    JOIN natural_person np ON ic.person_id = np.person_id
                    WHERE EXTRACT(MONTH FROM np.birthday) = $1
                    AND EXTRACT(DAY FROM np.birthday) = $2
                    AND ic.status = 'ACTIVE'
                    ORDER BY ic.vip_level DESC, ic.total_consumption DESC
                ''', target_date.month, target_date.day)
                customers.extend([dict(row) for row in rows])

            return customers

    async def get_customer_consumption_history(self, customer_id: str, institution_code: str, limit: int = 10):
        """获取客户消费历史"""
        suffix = institution_code.lower().replace('-', '_')
        order_table = f"consumption_record_{suffix}"
        inst_project_table = f"institution_project_{suffix}"
        inst_product_table = f"institution_product_{suffix}"
        inst_doctor_table = f"institution_doctor_{suffix}"

        async with self.pg.get_connection() as conn:
            rows = await conn.fetch(f'''
                SELECT 
                    co.order_number,
                    co.order_date,
                    co.order_type,
                    co.actual_amount,
                    co.payment_method,
                    co.notes,
                    p.name as project_name,
                    p.category as project_category,
                    pr.name as product_name,
                    pr.brand as product_brand,
                    d.name as doctor_name
                FROM {order_table} co
                LEFT JOIN {inst_project_table} ip ON co.institution_project_id = ip.institution_project_id
                LEFT JOIN project p ON ip.project_id = p.project_id
                LEFT JOIN {inst_product_table} ipr ON co.institution_product_id = ipr.institution_product_id
                LEFT JOIN product pr ON ipr.product_id = pr.product_id
                LEFT JOIN {inst_doctor_table} id ON co.institution_doctor_id = id.institution_doctor_id
                LEFT JOIN doctor d ON id.doctor_id = d.doctor_id
                WHERE co.institution_customer_id = $1
                ORDER BY co.order_date DESC
                LIMIT $2
            ''', customer_id, limit)

            return [dict(row) for row in rows]

    # ==================== NebulaGraph 同步 ====================

    def _escape_nebula(self, value) -> str:
        """转义 NebulaGraph 字符串值"""
        if value is None:
            return '""'
        s = str(value)
        s = s.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
        return f'"{s}"'

    def sync_to_nebula_institution(self, data: Dict, institution_id: str):
        """同步机构到 NebulaGraph"""
        try:
            execute_ngql(f"USE {settings.DATABASE.NEBULA_SPACE}")
            ngql = f'''
                INSERT VERTEX institution(institution_id, institution_code, name, alias, type, status)
                VALUES "{institution_id}": ("{institution_id}", "{data['institution_code']}", 
                    {self._escape_nebula(data['name'])}, {self._escape_nebula(data.get('alias'))}, 
                    {self._escape_nebula(data.get('type'))}, "{data.get('status', 'ACTIVE')}")
            '''
            result = execute_ngql(ngql)
            if result and result.is_succeeded():
                logger.debug(f"✅ 同步机构到 NebulaGraph: {data['institution_code']}")
            else:
                logger.warning(f"同步机构到 NebulaGraph 失败: {result.error_msg() if result else 'Unknown'}")
        except Exception as e:
            logger.error(f"同步机构到 NebulaGraph 异常: {e}")

    def sync_to_nebula_doctor(self, data: Dict, doctor_id: str):
        """同步医生到 NebulaGraph"""
        try:
            execute_ngql(f"USE {settings.DATABASE.NEBULA_SPACE}")
            specialty_str = ','.join(data.get('specialty', []))
            ngql = f'''
                INSERT VERTEX doctor(doctor_id, doctor_code, name, gender, phone, 
                    institution_code, title, specialty, introduction)
                VALUES "{doctor_id}": ("{doctor_id}", "{data['doctor_code']}", 
                    {self._escape_nebula(data['name'])}, "{data.get('gender', '')}", 
                    "{data.get('phone', '')}", "{data.get('institution_code', '')}", 
                    {self._escape_nebula(data.get('title'))}, {self._escape_nebula(specialty_str)}, 
                    {self._escape_nebula(data.get('introduction'))})
            '''
            result = execute_ngql(ngql)
            if result and result.is_succeeded():
                logger.debug(f"✅ 同步医生到 NebulaGraph: {data['doctor_code']}")

                # 创建医生与机构的边
                if data.get('institution_code'):
                    edge_ngql = f'''
                        INSERT EDGE doctor_works_at_institution(status) 
                        VALUES "{doctor_id}" -> "{data['institution_code']}": ("ACTIVE")
                    '''
                    execute_ngql(edge_ngql)
        except Exception as e:
            logger.error(f"同步医生到 NebulaGraph 异常: {e}")

    def sync_to_nebula_project(self, data: Dict, project_id: str):
        """同步项目到 NebulaGraph"""
        try:
            execute_ngql(f"USE {settings.DATABASE.NEBULA_SPACE}")
            ngql = f'''
                INSERT VERTEX project(project_id, project_code, name, category, body_part, risk_level)
                VALUES "{project_id}": ("{project_id}", "{data['project_code']}", 
                    {self._escape_nebula(data['name'])}, {self._escape_nebula(data.get('category'))}, 
                    {self._escape_nebula(data.get('body_part'))}, {data.get('risk_level', 1)})
            '''
            result = execute_ngql(ngql)
            if result and result.is_succeeded():
                logger.debug(f"✅ 同步项目到 NebulaGraph: {data['project_code']}")
        except Exception as e:
            logger.error(f"同步项目到 NebulaGraph 异常: {e}")

    def sync_to_nebula_product(self, data: Dict, product_id: str):
        """同步产品到 NebulaGraph"""
        try:
            execute_ngql(f"USE {settings.DATABASE.NEBULA_SPACE}")
            ngql = f'''
                INSERT VERTEX product(product_id, product_code, name, brand, category, body_part)
                VALUES "{product_id}": ("{product_id}", "{data['product_code']}", 
                    {self._escape_nebula(data['name'])}, {self._escape_nebula(data.get('brand'))}, 
                    {self._escape_nebula(data.get('category'))}, {self._escape_nebula(data.get('body_part'))})
            '''
            result = execute_ngql(ngql)
            if result and result.is_succeeded():
                logger.debug(f"✅ 同步产品到 NebulaGraph: {data['product_code']}")
        except Exception as e:
            logger.error(f"同步产品到 NebulaGraph 异常: {e}")

    def sync_to_nebula_customer(self, data: Dict, ids: Dict[str, str], institution_code: str):
        """同步客户到 NebulaGraph"""
        try:
            execute_ngql(f"USE {settings.DATABASE.NEBULA_SPACE}")
            person_data = data.get('person', {})
            inst_customer_id = ids['institution_customer_id']

            ngql = f'''
                INSERT VERTEX institution_customer(
                    institution_customer_id, customer_code, name, phone, gender, birthday)
                VALUES "{inst_customer_id}": ("{inst_customer_id}", "{data['customer_code']}", 
                    {self._escape_nebula(person_data.get('name'))}, "{person_data.get('phone', '')}", 
                    "{person_data.get('gender', '')}", "{person_data.get('birthday', '')}")
            '''
            result = execute_ngql(ngql)
            if result and result.is_succeeded():
                logger.debug(f"✅ 同步客户到 NebulaGraph: {data['customer_code']}")

                # 创建客户与机构的边
                inst_id = ids['institution_id']
                edge_ngql = f'''
                    INSERT EDGE customer_belongs_to_institution(customer_code, vip_level, status) 
                    VALUES "{inst_customer_id}" -> "{inst_id}": ("{data['customer_code']}", "{data.get('vip_level', 'NORMAL')}", "{data.get('status', 'ACTIVE')}")
                '''
                execute_ngql(edge_ngql)

        except Exception as e:
            logger.error(f"同步客户到 NebulaGraph 异常: {e}")

    # ==================== Qdrant 同步 ====================

    def sync_to_qdrant_project(self, data: Dict, project_id: str):
        """同步项目到 Qdrant（向量化存储）"""
        try:
            if not self._qdrant_client:
                logger.warning("Qdrant 客户端未初始化")
                return

            # 构建文本用于向量化
            text = f"{data['name']} {data.get('category', '')} {data.get('body_part', '')} {data.get('description', '')} {data.get('indications', '')}"

            from qdrant_client.models import PointStruct
            import hashlib

            point_id = int(hashlib.md5(project_id.encode()).hexdigest()[:16], 16)

            self._qdrant_client.upsert(
                collection_name="medical_knowledge",
                points=[PointStruct(
                    id=point_id,
                    vector=[0.0] * 1536,  # 占位向量，实际应用时需要真正的 embedding
                    payload={
                        "type": "project",
                        "id": project_id,
                        "code": data['project_code'],
                        "name": data['name'],
                        "category": data.get('category'),
                        "body_part": data.get('body_part'),
                        "description": data.get('description'),
                        "text": text
                    }
                )]
            )
            logger.debug(f"✅ 同步项目到 Qdrant: {data['project_code']}")
        except Exception as e:
            logger.error(f"同步项目到 Qdrant 异常: {e}")

    def sync_to_qdrant_product(self, data: Dict, product_id: str):
        """同步产品到 Qdrant"""
        try:
            if not self._qdrant_client:
                return

            text = f"{data['name']} {data.get('brand', '')} {data.get('category', '')} {data.get('description', '')}"

            from qdrant_client.models import PointStruct
            import hashlib

            point_id = int(hashlib.md5(product_id.encode()).hexdigest()[:16], 16)

            self._qdrant_client.upsert(
                collection_name="medical_knowledge",
                points=[PointStruct(
                    id=point_id,
                    vector=[0.0] * 1536,
                    payload={
                        "type": "product",
                        "id": product_id,
                        "code": data['product_code'],
                        "name": data['name'],
                        "brand": data.get('brand'),
                        "category": data.get('category'),
                        "description": data.get('description'),
                        "text": text
                    }
                )]
            )
            logger.debug(f"✅ 同步产品到 Qdrant: {data['product_code']}")
        except Exception as e:
            logger.error(f"同步产品到 Qdrant 异常: {e}")

    def sync_to_qdrant_customer(self, data: Dict, ids: Dict[str, str]):
        """同步客户画像到 Qdrant"""
        try:
            if not self._qdrant_client:
                return

            person_data = data.get('person', {})
            text = f"{person_data.get('name', '')} {data.get('vip_level', '')} {data.get('status', '')}"

            from qdrant_client.models import PointStruct
            import hashlib

            inst_customer_id = ids['institution_customer_id']
            point_id = int(hashlib.md5(inst_customer_id.encode()).hexdigest()[:16], 16)

            self._qdrant_client.upsert(
                collection_name="customer_profiles",
                points=[PointStruct(
                    id=point_id,
                    vector=[0.0] * 1536,
                    payload={
                        "type": "customer",
                        "id": inst_customer_id,
                        "code": data['customer_code'],
                        "name": person_data.get('name'),
                        "vip_level": data.get('vip_level'),
                        "status": data.get('status'),
                        "text": text
                    }
                )]
            )
            logger.debug(f"✅ 同步客户到 Qdrant: {data['customer_code']}")
        except Exception as e:
            logger.error(f"同步客户到 Qdrant 异常: {e}")

    # ==================== ClickHouse 同步 ====================

    async def sync_to_clickhouse_institution(self, data: Dict, institution_id: str):
        """同步机构到 ClickHouse"""
        try:
            alias = data.get('alias', '') or ''
            inst_type = data.get('type', '') or ''
            query = f'''
                INSERT INTO {settings.DATABASE.CLICKHOUSE_DB}.dim_institution 
                    (institution_id, institution_code, name, alias, type, status)
                VALUES ('{institution_id}', '{data['institution_code']}', 
                    '{self._escape_ch(data['name'])}', '{self._escape_ch(alias)}', 
                    '{self._escape_ch(inst_type)}', '{data.get('status', 'ACTIVE')}')
            '''
            await ch_execute_query(query)
            logger.debug(f"✅ 同步机构到 ClickHouse: {data['institution_code']}")
        except Exception as e:
            logger.error(f"同步机构到 ClickHouse 异常: {e}")

    def _escape_ch(self, value) -> str:
        """转义 ClickHouse 字符串"""
        if value is None:
            return ''
        return str(value).replace("'", "\\'").replace("\\", "\\\\")

    async def sync_to_clickhouse_project(self, data: Dict, project_id: str):
        """同步项目到 ClickHouse"""
        try:
            query = f'''
                INSERT INTO {settings.DATABASE.CLICKHOUSE_DB}.dim_project 
                    (project_id, project_code, name, category, body_part, risk_level, description)
                VALUES ('{project_id}', '{data['project_code']}', 
                    '{self._escape_ch(data['name'])}', '{self._escape_ch(data.get('category'))}', 
                    '{self._escape_ch(data.get('body_part'))}', {data.get('risk_level') or 1}, 
                    '{self._escape_ch(data.get('description'))}')
            '''
            await ch_execute_query(query)
            logger.debug(f"✅ 同步项目到 ClickHouse: {data['project_code']}")
        except Exception as e:
            logger.error(f"同步项目到 ClickHouse 异常: {e}")

    async def sync_to_clickhouse_product(self, data: Dict, product_id: str):
        """同步产品到 ClickHouse"""
        try:
            query = f'''
                INSERT INTO {settings.DATABASE.CLICKHOUSE_DB}.dim_product 
                    (product_id, product_code, name, brand, category, body_part, description)
                VALUES ('{product_id}', '{data['product_code']}', 
                    '{self._escape_ch(data['name'])}', '{self._escape_ch(data.get('brand'))}', 
                    '{self._escape_ch(data.get('category'))}', '{self._escape_ch(data.get('body_part'))}', 
                    '{self._escape_ch(data.get('description'))}')
            '''
            await ch_execute_query(query)
            logger.debug(f"✅ 同步产品到 ClickHouse: {data['product_code']}")
        except Exception as e:
            logger.error(f"同步产品到 ClickHouse 异常: {e}")

    async def sync_to_clickhouse_doctor(self, data: Dict, doctor_id: str):
        """同步医生到 ClickHouse"""
        try:
            specialty = data.get('specialty', [])
            if isinstance(specialty, list):
                specialty_str = "['{}']".format("','".join(specialty))
            else:
                specialty_str = f"['{specialty}']"

            query = f'''
                INSERT INTO {settings.DATABASE.CLICKHOUSE_DB}.dim_doctor 
                    (doctor_id, doctor_code, name, gender, phone, institution_code, title, specialty, introduction)
                VALUES ('{doctor_id}', '{data['doctor_code']}', 
                    '{self._escape_ch(data['name'])}', '{data.get('gender', '')}', 
                    '{data.get('phone', '')}', '{data.get('institution_code', '')}', 
                    '{self._escape_ch(data.get('title'))}', {specialty_str}, 
                    '{self._escape_ch(data.get('introduction'))}')
            '''
            await ch_execute_query(query)
            logger.debug(f"✅ 同步医生到 ClickHouse: {data['doctor_code']}")
        except Exception as e:
            logger.error(f"同步医生到 ClickHouse 异常: {e}")

    async def sync_to_clickhouse_consumption(self, data: Dict, institution_code: str):
        """同步消费记录到 ClickHouse"""
        try:
            order_date = data.get('order_date', '')
            query = f'''
                INSERT INTO {settings.DATABASE.CLICKHOUSE_DB}.fact_consumption 
                    (order_number, institution_code, customer_code, doctor_code,
                     order_date, order_type, project_code, product_code,
                     total_amount, discount_amount, actual_amount, payment_method, payment_status)
                VALUES ('{data['order_number']}', '{institution_code}', '{data['customer_code']}', 
                    '{data.get('doctor_code', '')}', '{order_date}', '{data.get('order_type', '')}',
                    '{data.get('project_code', '')}', '{data.get('product_code', '')}',
                    {data.get('total_amount', 0)}, {data.get('discount_amount', 0)}, 
                    {data.get('actual_amount', 0)}, '{data.get('payment_method', '')}',
                    '{data.get('payment_status', 'PAID')}')
            '''
            await ch_execute_query(query)
            logger.debug(f"✅ 同步消费记录到 ClickHouse: {data['order_number']}")
        except Exception as e:
            logger.error(f"同步消费记录到 ClickHouse 异常: {e}")

    async def sync_to_clickhouse_customer(self, customer_data: Dict, institution_code: str):
        """同步客户到 ClickHouse"""
        try:
            # 处理生日
            birthday = customer_data.get('birthday')
            birthday_str = f"'{birthday}'" if birthday else 'NULL'

            # 处理日期
            first_visit = customer_data.get('first_visit_date')
            first_visit_str = f"'{first_visit}'" if first_visit else 'NULL'

            last_visit = customer_data.get('last_visit_date')
            last_visit_str = f"'{last_visit}'" if last_visit else 'NULL'

            query = f'''
                INSERT INTO {settings.DATABASE.CLICKHOUSE_DB}.dim_customer 
                    (institution_customer_id, person_id, customer_code, name, phone, gender, birthday,
                     institution_id, institution_code, vip_level, status,
                     first_visit_date, last_visit_date, consumption_count, total_consumption,
                     referrer_id, doctor_id)
                VALUES (
                    '{customer_data['institution_customer_id']}', 
                    '{customer_data['person_id']}',
                    '{customer_data['customer_code']}', 
                    '{self._escape_ch(customer_data.get('name', ''))}',
                    '{customer_data.get('phone', '')}', 
                    '{customer_data.get('gender', '')}',
                    {birthday_str},
                    '{customer_data.get('institution_id', '')}', 
                    '{institution_code}',
                    '{customer_data.get('vip_level', 'NORMAL')}', 
                    '{customer_data.get('status', 'ACTIVE')}',
                    {first_visit_str}, 
                    {last_visit_str},
                    {customer_data.get('consumption_count', 0)}, 
                    {customer_data.get('total_consumption', 0)},
                    '{customer_data.get('referrer_id', '')}', 
                    '{customer_data.get('doctor_id', '')}'
                )
            '''
            await ch_execute_query(query)
            logger.debug(f"✅ 同步客户到 ClickHouse: {customer_data['customer_code']}")
        except Exception as e:
            logger.error(f"同步客户到 ClickHouse 异常: {e}")

    async def sync_existing_customers_to_clickhouse(self):
        """同步所有现有客户数据到 ClickHouse（用于修复遗漏数据）"""
        logger.info("开始同步现有客户数据到 ClickHouse...")
        synced_count = 0

        async with self.pg.get_connection() as conn:
            # 获取所有机构
            institutions = await conn.fetch('SELECT institution_code FROM institution')

            for inst in institutions:
                institution_code = inst['institution_code']
                suffix = institution_code.lower().replace('-', '_')
                customer_table = f"institution_customer_{suffix}"

                try:
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

                    # 获取该机构的所有客户
                    customers = await conn.fetch(f'''
                        SELECT 
                            ic.institution_customer_id::text,
                            ic.institution_id::text,
                            ic.person_id::text,
                            ic.customer_code,
                            ic.vip_level,
                            ic.status,
                            ic.first_visit_date,
                            ic.last_visit_date,
                            ic.consumption_count,
                            ic.total_consumption,
                            ic.referrer_id::text,
                            ic.doctor_id::text,
                            np.name,
                            np.phone,
                            np.gender,
                            np.birthday
                        FROM {customer_table} ic
                        JOIN natural_person np ON ic.person_id = np.person_id
                    ''')

                    for customer in customers:
                        customer_data = dict(customer)
                        # 转换日期格式
                        if customer_data.get('birthday'):
                            customer_data['birthday'] = customer_data['birthday'].strftime('%Y-%m-%d')
                        if customer_data.get('first_visit_date'):
                            customer_data['first_visit_date'] = customer_data['first_visit_date'].strftime('%Y-%m-%d')
                        if customer_data.get('last_visit_date'):
                            customer_data['last_visit_date'] = customer_data['last_visit_date'].strftime('%Y-%m-%d')

                        await self.sync_to_clickhouse_customer(customer_data, institution_code)
                        synced_count += 1

                except Exception as e:
                    logger.error(f"同步机构 {institution_code} 客户时出错: {e}")

        logger.info(f"✅ 客户数据同步完成，共同步 {synced_count} 条记录")
        return synced_count


# 全局实例
data_sync_service = DataSyncService()

