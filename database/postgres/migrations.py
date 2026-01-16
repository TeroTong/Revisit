"""
数据库迁移脚本 - 与 models.py 保持一致的版本

注意：示例数据默认不插入，仅在开发/测试环境使用 --with-sample-data 参数时才插入
"""

import asyncio
import asyncpg
import logging

from config.settings import settings
from database.postgres.models import ( DatabaseInitializer
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseMigrator:
    """数据库迁移器 - 使用新的 models.py 结构"""

    def __init__(self, connection: asyncpg.Connection, with_sample_data: bool = False):
        """
        初始化迁移器

        Args:
            connection: 数据库连接
            with_sample_data: 是否插入示例数据（默认 False，仅用于开发/测试）
        """
        self.conn = connection
        self.with_sample_data = with_sample_data
        self.created_tables = []

    async def migrate_all(self):
        """执行所有迁移"""
        logger.info("🚀 开始数据库迁移...")

        try:
            # 1. 创建扩展
            await self._create_extensions()

            # 2. 使用 DatabaseInitializer 创建基础表
            await DatabaseInitializer.initialize_database(self.conn)

            # 3. 插入初始数据（仅当明确指定时）
            if self.with_sample_data:
                logger.warning("⚠️ 正在插入示例数据（仅用于开发/测试环境）...")
                await self._insert_sample_data()
            else:
                logger.info("📝 跳过示例数据插入（生产环境模式）")
                # 仅插入必要的机构数据（如果需要创建机构表）
                await self._ensure_institutions_exist()

            # 4. 创建机构特定表
            await self._create_institution_tables()

            logger.info("🎉 数据库迁移完成！")

            # 显示创建的表
            await self._show_created_tables()

        except Exception as e:
            logger.error(f"❌ 迁移失败: {e}")
            import traceback
            traceback.print_exc()
            raise

    async def _create_extensions(self):
        """创建必要的扩展"""
        logger.info("🔧 正在创建扩展...")

        try:
            await self.conn.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')
            await self.conn.execute('CREATE EXTENSION IF NOT EXISTS "pgcrypto";')
            logger.info("✅ 扩展创建完成")
        except Exception as e:
            logger.warning(f"⚠️ 创建扩展失败: {e}，继续执行...")

    async def _ensure_institutions_exist(self):
        """确保配置的机构存在（不插入示例数据时使用）"""
        institution_codes = getattr(settings.APP, 'INSTITUTIONS', ['BJ-HA-001', 'SH-ML-002'])

        for code in institution_codes:
            # 检查机构是否存在
            exists = await self.conn.fetchval(
                'SELECT EXISTS(SELECT 1 FROM institution WHERE institution_code = $1)',
                code
            )

            if not exists:
                # 插入最小化的机构记录（仅 code，其他信息后续补充）
                await self.conn.execute('''
                    INSERT INTO institution (institution_code, name, status)
                    VALUES ($1, $2, 'ACTIVE')
                    ON CONFLICT (institution_code) DO NOTHING
                ''', code, f'机构 {code}')
                logger.info(f"✅ 创建机构占位记录: {code}")

    async def _insert_sample_data(self):
        """插入示例数据（仅用于开发/测试）"""
        logger.info("📝 正在插入示例数据...")

        # 1. 插入示例机构数据
        await self._insert_sample_institutions()

        # 2. 插入示例项目
        await self._insert_sample_projects()

        # 3. 插入示例产品
        await self._insert_sample_products()

        # 4. 插入示例医生
        await self._insert_sample_doctors()

        # 5. 插入示例关系
        await self._insert_sample_medical_relations()

    async def _insert_sample_institutions(self):
        """插入示例机构数据"""
        institutions = [
            {
                "institution_code": "BJ-HA-001",
                "name": "北京华美医疗美容医院",
                "alias": "华美北京总院",
                "type": "HOSPITAL",
                "status": "ACTIVE"
            },
            {
                "institution_code": "SH-ML-002",
                "name": "上海美莱医疗美容医院",
                "alias": "美莱上海分院",
                "type": "CLINIC",
                "status": "ACTIVE"
            }
        ]

        try:
            for inst in institutions:
                await self.conn.execute('''
                    INSERT INTO institution (institution_code, name, alias, type, status)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (institution_code) DO UPDATE SET
                    name = EXCLUDED.name,
                    alias = EXCLUDED.alias,
                    type = EXCLUDED.type,
                    status = EXCLUDED.status,
                    updated_at = CURRENT_TIMESTAMP
                ''', inst["institution_code"], inst["name"], inst["alias"],
                                        inst["type"], inst["status"])

            logger.info(f"✅ 插入了 {len(institutions)} 条机构数据")

        except Exception as e:
            logger.error(f"❌ 插入机构数据失败: {e}")

    async def _insert_sample_projects(self):
        """插入示例项目"""
        projects = [
            ("BOTOX-001", "肉毒素注射", "INJECTION", 2, "祛除面部动态皱纹"),
            ("BOTOX-002", "肉毒素注射", "INJECTION", 3, "祛除面部动态皱纹"),
            ("FILLER-001", "玻尿酸填充", "INJECTION", 2, "面部轮廓塑形和填充"),
            ("FILLER-002", "玻尿酸填充", "INJECTION", 3, "面部轮廓塑形和填充"),
            ("LASER-001", "激光祛斑", "LASER", 1, "色素性皮肤问题治疗"),
            ("THERMAGE-001", "热玛吉", "RADIOFREQUENCY", 2, "皮肤紧致提升"),
            ("ULTHERA-001", "超声刀", "EQUIPMENT", 2, "皮肤紧致提升"),
            ("MICRO-001", "微针", "MICRO-NEEDLING", 1, "皮肤再生治疗")
        ]

        try:
            for proj in projects:
                await self.conn.execute('''
                    INSERT INTO project (project_code, name, category, risk_level, description)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (project_code) DO UPDATE SET
                    name = EXCLUDED.name,
                    category = EXCLUDED.category,
                    risk_level = EXCLUDED.risk_level,
                    description = EXCLUDED.description
                ''', *proj)

            logger.info(f"✅ 插入了 {len(projects)} 条项目数据")

        except Exception as e:
            logger.error(f"❌ 插入项目数据失败: {e}")

    async def _insert_sample_products(self):
        """插入示例产品"""
        # 字段顺序: product_code, name, brand, category, unit, description
        products = [
            ("BOTOX-100U", "保妥适", "艾尔建", "INJECTION", "瓶", "进口肉毒素，100单位/瓶"),
            ("JUVEDERM-001", "乔雅登", "艾尔建", "INJECTION", "支", "玻尿酸填充剂，1ml/支"),
            ("PICOWAY", "皮秒激光", "赛诺秀", "EQUIPMENT", "台", "皮秒激光设备"),
            ("ULTHERA", "超声刀", "Ulthera", "EQUIPMENT", "台", "超声紧肤设备"),
            ("RESTYLANE", "瑞蓝", "高德美", "INJECTION", "支", "玻尿酸填充剂，1ml/支")
        ]

        try:
            for prod in products:
                await self.conn.execute('''
                    INSERT INTO product (product_code, name, brand, category, unit, description)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (product_code) DO UPDATE SET
                    name = EXCLUDED.name,
                    brand = EXCLUDED.brand,
                    category = EXCLUDED.category,
                    unit = EXCLUDED.unit,
                    description = EXCLUDED.description
                ''', *prod)

            logger.info(f"✅ 插入了 {len(products)} 条产品数据")

        except Exception as e:
            logger.error(f"❌ 插入产品数据失败: {e}")

    async def _insert_sample_doctors(self):
        """插入示例医生"""
        # 字段顺序: doctor_code, name, gender, phone, institution_code, title, specialty, introduction
        doctors = [
            ("DOC-001", "张医生", "MALE", "13800138001", "BJ-HA-001", "主任医师",
             ["眼部整形", "鼻部整形"], "资深整形外科专家，从业20年"),
            ("DOC-002", "李医生", "FEMALE", "13900139002", "SH-ML-002", "副主任医师",
             ["皮肤美容", "激光治疗"], "皮肤科专家，擅长各种激光治疗"),
            ("DOC-003", "王医生", "MALE", "13600136003", "BJ-HA-001", "主任医师",
             ["胸部整形", "形体雕塑"], "形体雕塑专家"),
            ("DOC-004", "陈医生", "FEMALE", "13700137004", "SH-ML-002", "主治医师",
             ["微整形", "注射美容"], "注射美容专家")
        ]

        try:
            for doc in doctors:
                await self.conn.execute('''
                    INSERT INTO doctor (doctor_code, name, gender, phone,
                                        institution_code, title, specialty, introduction)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (doctor_code) DO UPDATE SET
                    name = EXCLUDED.name,
                    gender = EXCLUDED.gender,
                    phone = EXCLUDED.phone,
                    institution_code = EXCLUDED.institution_code,
                    title = EXCLUDED.title,
                    specialty = EXCLUDED.specialty,
                    introduction = EXCLUDED.introduction
                ''', *doc)

            logger.info(f"✅ 插入了 {len(doctors)} 条医生数据")

        except Exception as e:
            logger.error(f"❌ 插入医生数据失败: {e}")

    async def _insert_sample_medical_relations(self):
        """插入示例医美关系数据"""
        logger.info("插入医美关系数据...")

        try:
            # 首先获取项目ID
            projects = await self.conn.fetch('SELECT project_id, project_code FROM project')
            project_map = {p['project_code']: p['project_id'] for p in projects}

            # 获取产品ID
            products = await self.conn.fetch('SELECT product_id, product_code FROM product')
            product_map = {p['product_code']: p['product_id'] for p in products}

            # 定义关系数据
            relations_data = [
                # 格式: (source_type, source_code, target_type, target_code, relation_type, level, desc, bidirectional)
                ('PROJECT', 'BOTOX-001', 'PROJECT', 'BOTOX-002', 'UPGRADE', 3,
                 '从基础肉毒素升级到高级版本，效果更持久', False),

                ('PROJECT', 'FILLER-001', 'PROJECT', 'FILLER-002', 'SIMILAR', 4,
                 '两种玻尿酸填充剂，效果类似但分子量不同', True),

                ('PROJECT', 'LASER-001', 'PRODUCT', 'PICOWAY', 'PREREQUISITE', 5,
                 '激光祛斑项目需要使用皮秒激光设备', False),

                ('PRODUCT', 'BOTOX-100U', 'PRODUCT', 'JUVEDERM-001', 'COMBINATION', 5,
                 '肉毒素+玻尿酸联合治疗，实现综合年轻化', True),

                ('PRODUCT', 'JUVEDERM-001', 'PRODUCT', 'RESTYLANE', 'ALTERNATIVE', 3,
                 '两种玻尿酸品牌，可根据客户偏好选择', True)
            ]

            inserted_count = 0
            for rel in relations_data:
                source_type, source_code, target_type, target_code, rel_type, level, desc, bidirectional = rel

                # 获取源ID
                if source_type == 'PROJECT':
                    source_id = project_map.get(source_code)
                else:
                    source_id = product_map.get(source_code)

                # 获取目标ID
                if target_type == 'PROJECT':
                    target_id = project_map.get(target_code)
                else:
                    target_id = product_map.get(target_code)

                if not source_id or not target_id:
                    logger.warning(f"无法找到关系数据中的项目/产品: {source_code} -> {target_code}")
                    continue

                # 插入关系数据
                await self.conn.execute('''
                    INSERT INTO medical_relation 
                    (source_type, source_id, target_type, target_id, relation_type, 
                     relation_level, description, is_bidirectional)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (source_type, source_id, target_type, target_id, relation_type) 
                    DO UPDATE SET
                    relation_level = EXCLUDED.relation_level,
                    description = EXCLUDED.description,
                    updated_at = CURRENT_TIMESTAMP
                ''', source_type, source_id, target_type, target_id, rel_type,
                                        level, desc, bidirectional)

                inserted_count += 1

            logger.info(f"✅ 插入了 {inserted_count} 条医美关系数据")

        except Exception as e:
            logger.error(f"❌ 插入医美关系数据失败: {e}")

    async def _create_institution_tables(self):
        """创建机构特定表"""
        logger.info("🏥 正在创建机构特定表...")

        # 从配置获取机构列表
        institution_codes = getattr(settings.APP, 'INSTITUTIONS', ['BJ-HA-001', 'SH-ML-002'])

        success_count = 0
        for institution_code in institution_codes:
            logger.info(f"创建机构 {institution_code} 的表...")

            try:
                success = await DatabaseInitializer.create_institution_tables(
                    self.conn, institution_code
                )
                if success:
                    logger.info(f"✅ 机构 {institution_code} 的表创建完成")
                    success_count += 1
                else:
                    logger.warning(f"⚠️ 机构 {institution_code} 的表创建失败")

            except Exception as e:
                logger.error(f"❌ 创建机构 {institution_code} 的表失败: {e}")
                # 继续创建其他机构的表
                continue

        logger.info(f"✅ 成功为 {success_count}/{len(institution_codes)} 个机构创建了表")

    async def _show_created_tables(self):
        """显示所有创建的表"""
        logger.info("📊 数据库状态报告...")

        try:
            # 获取所有表
            tables = await self.conn.fetch("""
                SELECT table_name, 
                       (SELECT COUNT(*) FROM information_schema.columns 
                        WHERE table_name = t.table_name) as column_count
                FROM information_schema.tables t
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)

            logger.info(f"📋 共创建 {len(tables)} 张表:")
            for table in tables:
                logger.info(f"  - {table['table_name']} ({table['column_count']} 列)")

            # 显示机构特定表
            institution_tables = await self.conn.fetch("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND (table_name LIKE '%_bj_ha_001' OR table_name LIKE '%_sh_ml_002')
                ORDER BY table_name
            """)

            if institution_tables:
                logger.info(f"🏥 机构特定表 ({len(institution_tables)} 张):")
                for table in institution_tables:
                    logger.info(f"  - {table['table_name']}")

        except Exception as e:
            logger.error(f"❌ 获取表信息失败: {e}")


async def run_migration():
    """运行迁移"""
    from database.postgres.connection import create_pool

    try:
        pool = await create_pool()

        async with pool.acquire() as conn:
            migrator = DatabaseMigrator(conn)
            await migrator.migrate_all()

    except Exception as e:
        logger.error(f"🚨 迁移过程出错: {e}")
        raise


if __name__ == "__main__":
    print("=" * 60)
    print("数据库迁移脚本")
    print("与 models.py 保持一致的版本")
    print("=" * 60)

    asyncio.run(run_migration())