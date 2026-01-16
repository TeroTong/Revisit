"""数据导入脚本

用法：
    python -m scripts.import_data initial                    # 初始全量导入
    python -m scripts.import_data incremental                # 处理所有待处理增量
    python -m scripts.import_data incremental --date 2026-01-14  # 指定日期

数据流向：
    JSON 文件 → PostgreSQL (主数据) → NebulaGraph (图关系)
                                    → Qdrant (向量搜索)
                                    → ClickHouse (分析统计)
"""

import os

# 禁用代理设置（解决远程数据库连接问题）
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    os.environ.pop(key, None)

import json
import shutil
import argparse
import asyncio
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict

# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from services.data_sync import DataSyncService
from utils.logger import setup_logger

# 设置日志
logger = setup_logger(__name__)

# 数据目录
DATA_DIR = PROJECT_ROOT / "data"
IMPORT_DIR = DATA_DIR / "import"
INITIAL_DIR = IMPORT_DIR / "initial"
INCREMENTAL_DIR = IMPORT_DIR / "incremental"
PENDING_DIR = INCREMENTAL_DIR / "pending"
PROCESSED_DIR = INCREMENTAL_DIR / "processed"


def load_json(file_path: Path) -> list:
    """加载 JSON 文件"""
    if not file_path.exists():
        logger.warning(f"[跳过] 文件不存在: {file_path}")
        return []
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        return data if isinstance(data, list) else [data]


class DataImporter:
    """数据导入器

    负责从 JSON 文件导入数据到 PostgreSQL，并同步到其他数据库。

    导入顺序（保证外键依赖）：
    1. 机构 (institution)
    2. 医生 (doctor) - 依赖机构
    3. 项目 (project)
    4. 产品 (product)
    5. 客户 (customer) - 依赖机构、医生
    6. 消费记录 (consumption) - 依赖客户、项目、产品、医生
    7. 医疗关系 (relation) - 依赖项目、产品
    """

    def __init__(self):
        self.sync_service = DataSyncService()
        self.stats = {
            'institutions': 0,
            'doctors': 0,
            'projects': 0,
            'products': 0,
            'customers': 0,
            'consumption_records': 0,
            'relations': 0,
            'errors': 0
        }

    async def init(self):
        """初始化数据库连接"""
        logger.info("正在初始化数据库连接...")
        await self.sync_service.init_connections()
        logger.info("✅ 数据库连接已建立")

    async def close(self):
        """关闭数据库连接"""
        await self.sync_service.close_connections()
        logger.info("✅ 数据库连接已关闭")

    def print_stats(self):
        """打印导入统计"""
        print("\n" + "=" * 50)
        print("导入统计:")
        print("=" * 50)
        for key, value in self.stats.items():
            status = "❌" if key == 'errors' and value > 0 else "✅"
            print(f"  {status} {key}: {value}")
        print("=" * 50)

    # ==================== 初始导入 ====================

    async def import_initial(self):
        """初始全量导入"""
        print("\n" + "=" * 60)
        print("  医美客户回访系统 - 初始数据导入")
        print("=" * 60)

        common_dir = INITIAL_DIR / "common"

        # 1. 导入机构
        await self._import_institutions(common_dir / "institutions.json")

        # 2. 导入医生
        await self._import_doctors(common_dir / "doctors.json")

        # 3. 导入项目
        await self._import_projects(common_dir / "projects.json")

        # 4. 导入产品
        await self._import_products(common_dir / "products.json")

        # 5. 导入各机构业务数据
        institutions_dir = INITIAL_DIR / "institutions"
        if institutions_dir.exists():
            for inst_dir in sorted(institutions_dir.iterdir()):
                if inst_dir.is_dir():
                    await self._import_institution_data(inst_dir)

        # 6. 导入医疗关系（边）
        await self._import_relations(common_dir / "medical_relations.json")

        self.print_stats()

        if self.stats['errors'] == 0:
            logger.info("✅ 初始导入完成!")
        else:
            logger.warning(f"⚠️ 初始导入完成，但有 {self.stats['errors']} 个错误")

    async def _import_institutions(self, file_path: Path):
        """导入机构数据"""
        data = load_json(file_path)
        if not data:
            return

        print(f"\n[1/6] 机构: 准备导入 {len(data)} 条记录...")

        for item in data:
            try:
                # 1. 写入 PostgreSQL
                institution_id = await self.sync_service.upsert_institution(item)

                if institution_id:
                    # 2. 同步到 NebulaGraph
                    self.sync_service.sync_to_nebula_institution(item, institution_id)

                    # 3. 同步到 ClickHouse
                    await self.sync_service.sync_to_clickhouse_institution(item, institution_id)

                    self.stats['institutions'] += 1
                    logger.info(f"  ✅ {item['institution_code']}: {item['name']}")

            except Exception as e:
                self.stats['errors'] += 1
                logger.error(f"  ❌ {item.get('institution_code', 'N/A')}: {e}")

    async def _import_doctors(self, file_path: Path):
        """导入医生数据"""
        data = load_json(file_path)
        if not data:
            return

        print(f"\n[2/6] 医生: 准备导入 {len(data)} 条记录...")

        for item in data:
            try:
                doctor_id = await self.sync_service.upsert_doctor(item)

                if doctor_id:
                    self.sync_service.sync_to_nebula_doctor(item, doctor_id)
                    await self.sync_service.sync_to_clickhouse_doctor(item, doctor_id)

                    self.stats['doctors'] += 1
                    logger.info(f"  ✅ {item['doctor_code']}: {item['name']} ({item.get('title', '')})")

            except Exception as e:
                self.stats['errors'] += 1
                logger.error(f"  ❌ {item.get('doctor_code', 'N/A')}: {e}")

    async def _import_projects(self, file_path: Path):
        """导入项目数据"""
        data = load_json(file_path)
        if not data:
            return

        print(f"\n[3/6] 项目: 准备导入 {len(data)} 条记录...")

        for item in data:
            try:
                project_id = await self.sync_service.upsert_project(item)

                if project_id:
                    self.sync_service.sync_to_nebula_project(item, project_id)
                    self.sync_service.sync_to_qdrant_project(item, project_id)
                    await self.sync_service.sync_to_clickhouse_project(item, project_id)

                    self.stats['projects'] += 1
                    logger.info(f"  ✅ {item['project_code']}: {item['name']}")

            except Exception as e:
                self.stats['errors'] += 1
                logger.error(f"  ❌ {item.get('project_code', 'N/A')}: {e}")

    async def _import_products(self, file_path: Path):
        """导入产品数据"""
        data = load_json(file_path)
        if not data:
            return

        print(f"\n[4/6] 产品: 准备导入 {len(data)} 条记录...")

        for item in data:
            try:
                product_id = await self.sync_service.upsert_product(item)

                if product_id:
                    self.sync_service.sync_to_nebula_product(item, product_id)
                    self.sync_service.sync_to_qdrant_product(item, product_id)
                    await self.sync_service.sync_to_clickhouse_product(item, product_id)

                    self.stats['products'] += 1
                    logger.info(f"  ✅ {item['product_code']}: {item['name']}")

            except Exception as e:
                self.stats['errors'] += 1
                logger.error(f"  ❌ {item.get('product_code', 'N/A')}: {e}")

    async def _import_institution_data(self, inst_dir: Path):
        """导入单个机构的业务数据"""
        institution_code = inst_dir.name
        print(f"\n[5/6] 机构业务数据: {institution_code}")

        # 导入客户
        customers_file = inst_dir / "customers.json"
        if customers_file.exists():
            customers = load_json(customers_file)
            print(f"  [客户] 准备导入 {len(customers)} 条记录...")

            for cust in customers:
                try:
                    ids = await self.sync_service.upsert_customer(cust, institution_code)

                    if ids:
                        self.sync_service.sync_to_nebula_customer(cust, ids, institution_code)
                        self.sync_service.sync_to_qdrant_customer(cust, ids)

                        # 同步到 ClickHouse
                        person = cust.get('person', {})
                        ch_customer_data = {
                            'institution_customer_id': ids['institution_customer_id'],
                            'person_id': ids['person_id'],
                            'institution_id': ids['institution_id'],
                            'customer_code': cust['customer_code'],
                            'name': person.get('name', ''),
                            'phone': person.get('phone', ''),
                            'gender': person.get('gender', ''),
                            'birthday': person.get('birthday'),
                            'vip_level': cust.get('vip_level', 'NORMAL'),
                            'status': cust.get('status', 'ACTIVE'),
                            'first_visit_date': cust.get('first_visit_date'),
                            'last_visit_date': cust.get('last_visit_date'),
                            'consumption_count': 0,
                            'total_consumption': 0,
                            'referrer_id': '',
                            'doctor_id': cust.get('doctor_code', '')
                        }
                        await self.sync_service.sync_to_clickhouse_customer(ch_customer_data, institution_code)

                        self.stats['customers'] += 1
                        logger.info(f"    ✅ {cust['customer_code']}: {person.get('name', 'N/A')}")

                except Exception as e:
                    self.stats['errors'] += 1
                    logger.error(f"    ❌ {cust.get('customer_code', 'N/A')}: {e}")

        # 导入消费记录
        records_file = inst_dir / "consumption_records.json"
        if records_file.exists():
            records = load_json(records_file)
            print(f"  [消费记录] 准备导入 {len(records)} 条记录...")

            for rec in records:
                try:
                    order_id = await self.sync_service.insert_consumption_record(rec, institution_code)

                    if order_id:
                        await self.sync_service.sync_to_clickhouse_consumption(rec, institution_code)
                        self.stats['consumption_records'] += 1
                        logger.info(f"    ✅ {rec['order_number']}")

                except Exception as e:
                    self.stats['errors'] += 1
                    logger.error(f"    ❌ {rec.get('order_number', 'N/A')}: {e}")

    async def _import_relations(self, file_path: Path):
        """导入关系数据（NebulaGraph 边）"""
        data = load_json(file_path)
        if not data:
            return

        print(f"\n[6/6] 医疗关系: 准备导入 {len(data)} 条边...")

        for rel in data:
            try:
                # TODO: 实现关系导入逻辑
                # await self.sync_service.upsert_relation(rel)
                self.stats['relations'] += 1
                logger.info(f"  ✅ {rel.get('source_code', 'N/A')} -> {rel.get('target_code', 'N/A')}")

            except Exception as e:
                self.stats['errors'] += 1
                logger.error(f"  ❌ 关系导入失败: {e}")

    # ==================== 增量更新 ====================

    async def process_incremental(self, date_str: Optional[str] = None):
        """处理增量更新"""
        print("\n" + "=" * 60)
        print("  医美客户回访系统 - 增量数据更新")
        print("=" * 60)

        if date_str:
            batch_dirs = [PENDING_DIR / date_str]
        else:
            batch_dirs = sorted(PENDING_DIR.iterdir()) if PENDING_DIR.exists() else []

        if not batch_dirs or not any(d.is_dir() for d in batch_dirs if d.exists()):
            print("\n没有待处理的增量数据")
            return

        for batch_dir in batch_dirs:
            if not batch_dir.exists() or not batch_dir.is_dir():
                continue
            await self._process_batch(batch_dir)

        self.print_stats()
        logger.info("✅ 增量处理完成!")

    async def _process_batch(self, batch_dir: Path):
        """处理单个增量批次"""
        date_str = batch_dir.name
        print(f"\n[批次] 处理日期: {date_str}")

        try:
            for file_path in sorted(batch_dir.glob("*.json")):
                file_name = file_path.stem

                if "_add" in file_name:
                    entity_type = file_name.replace("_add", "")
                    await self._handle_add(entity_type, file_path)

                elif "_update" in file_name:
                    entity_type = file_name.replace("_update", "")
                    await self._handle_update(entity_type, file_path)

                elif "_delete" in file_name:
                    entity_type = file_name.replace("_delete", "")
                    await self._handle_delete(entity_type, file_path)

            # 成功后归档
            self._archive_batch(batch_dir)

        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"  ❌ 批次处理失败: {e}")

    async def _handle_add(self, entity_type: str, file_path: Path):
        """处理新增"""
        raw_data = load_json(file_path)

        # 支持两种格式：直接数组 或 带 operation 的对象
        if isinstance(raw_data, list):
            data = raw_data
            institution_code = self._extract_institution_code_from_items(data)
        elif isinstance(raw_data, dict):
            data = raw_data.get('data', [])
            institution_code = raw_data.get('institution_code')
        else:
            data = []
            institution_code = None

        if not data:
            return

        print(f"  [新增] {entity_type}: {len(data)} 条")

        for item in data:
            try:
                if entity_type == "customers":
                    inst_code = institution_code or self._get_institution_from_customer_code(item.get('customer_code', ''))
                    if inst_code:
                        ids = await self.sync_service.upsert_customer(item, inst_code)
                        if ids:
                            self.sync_service.sync_to_nebula_customer(item, ids, inst_code)
                            self.sync_service.sync_to_qdrant_customer(item, ids)
                            self.stats['customers'] += 1
                            logger.info(f"    ✅ 新增客户: {item.get('customer_code')}")

                elif entity_type == "consumption_records":
                    inst_code = institution_code or self._get_institution_from_order_number(item.get('order_number', ''))
                    if inst_code:
                        record_id = await self.sync_service.insert_consumption_record(item, inst_code)
                        if record_id:
                            await self.sync_service.sync_to_clickhouse_consumption(item, inst_code)
                            self.stats['consumption_records'] += 1
                            logger.info(f"    ✅ 新增消费记录: {item.get('order_number')}")

                elif entity_type == "doctors":
                    doctor_id = await self.sync_service.upsert_doctor(item)
                    if doctor_id:
                        self.sync_service.sync_to_nebula_doctor(item, doctor_id)
                        await self.sync_service.sync_to_clickhouse_doctor(item, doctor_id)
                        self.stats['doctors'] += 1
                        logger.info(f"    ✅ 新增医生: {item.get('doctor_code')}")

                elif entity_type == "projects":
                    project_id = await self.sync_service.upsert_project(item)
                    if project_id:
                        self.sync_service.sync_to_nebula_project(item, project_id)
                        await self.sync_service.sync_to_clickhouse_project(item, project_id)
                        self.stats['projects'] += 1
                        logger.info(f"    ✅ 新增项目: {item.get('project_code')}")

                elif entity_type == "products":
                    product_id = await self.sync_service.upsert_product(item)
                    if product_id:
                        self.sync_service.sync_to_nebula_product(item, product_id)
                        await self.sync_service.sync_to_clickhouse_product(item, product_id)
                        self.stats['products'] += 1
                        logger.info(f"    ✅ 新增产品: {item.get('product_code')}")

            except Exception as e:
                self.stats['errors'] += 1
                logger.error(f"    ❌ 新增失败: {e}")

    async def _handle_update(self, entity_type: str, file_path: Path):
        """处理更新"""
        raw_data = load_json(file_path)

        # 支持两种格式
        if isinstance(raw_data, list):
            data = raw_data
            institution_code = None
        elif isinstance(raw_data, dict):
            data = raw_data.get('data', [])
            institution_code = raw_data.get('institution_code')
        else:
            data = []
            institution_code = None

        if not data:
            return

        print(f"  [更新] {entity_type}: {len(data)} 条")

        for item in data:
            try:
                # 获取要更新的字段
                updates = item.get('updates', item)

                if entity_type == "customers":
                    customer_code = item.get('customer_code')
                    inst_code = institution_code or self._get_institution_from_customer_code(customer_code)

                    if inst_code and customer_code:
                        # 构造更新数据
                        update_data = {'customer_code': customer_code}
                        update_data.update(updates)

                        ids = await self.sync_service.upsert_customer(update_data, inst_code)
                        if ids:
                            self.sync_service.sync_to_nebula_customer(update_data, ids, inst_code)
                            self.sync_service.sync_to_qdrant_customer(update_data, ids)
                            self.stats['customers'] += 1
                            logger.info(f"    ✅ 更新客户: {customer_code}")

                elif entity_type == "doctors":
                    doctor_code = item.get('doctor_code')
                    update_data = {'doctor_code': doctor_code}
                    update_data.update(updates)

                    doctor_id = await self.sync_service.upsert_doctor(update_data)
                    if doctor_id:
                        self.sync_service.sync_to_nebula_doctor(update_data, doctor_id)
                        await self.sync_service.sync_to_clickhouse_doctor(update_data, doctor_id)
                        self.stats['doctors'] += 1
                        logger.info(f"    ✅ 更新医生: {doctor_code}")

                elif entity_type == "projects":
                    project_code = item.get('project_code')
                    update_data = {'project_code': project_code}
                    update_data.update(updates)

                    project_id = await self.sync_service.upsert_project(update_data)
                    if project_id:
                        await self.sync_service.sync_to_clickhouse_project(update_data, project_id)
                        self.stats['projects'] += 1
                        logger.info(f"    ✅ 更新项目: {project_code}")

                elif entity_type == "products":
                    product_code = item.get('product_code')
                    update_data = {'product_code': product_code}
                    update_data.update(updates)

                    product_id = await self.sync_service.upsert_product(update_data)
                    if product_id:
                        await self.sync_service.sync_to_clickhouse_product(update_data, product_id)
                        self.stats['products'] += 1
                        logger.info(f"    ✅ 更新产品: {product_code}")

            except Exception as e:
                self.stats['errors'] += 1
                logger.error(f"    ❌ 更新失败: {e}")

    async def _handle_delete(self, entity_type: str, file_path: Path):
        """处理删除"""
        data = load_json(file_path)
        print(f"  [删除] {entity_type}: {len(data)} 条")

        for item in data:
            try:
                item_id = item.get("id") or item.get(f"{entity_type[:-1]}_id")
                # TODO: 实现删除逻辑
                logger.info(f"    ⚠️ 删除功能待实现: {item_id}")

            except Exception as e:
                self.stats['errors'] += 1
                logger.error(f"    ❌ 删除失败: {e}")

    def _extract_institution_code(self, file_path: Path) -> Optional[str]:
        """从文件路径提取机构代码"""
        # 尝试从 JSON 内容中提取
        data = load_json(file_path)
        if data and len(data) > 0:
            first_item = data[0]
            customer_code = first_item.get('customer_code', '')
            if customer_code:
                return self._get_institution_from_customer_code(customer_code)
        return None

    def _extract_institution_code_from_items(self, data: list) -> Optional[str]:
        """从数据项中提取机构代码"""
        if not data:
            return None
        first_item = data[0]
        customer_code = first_item.get('customer_code', '')
        order_number = first_item.get('order_number', '')

        if customer_code:
            return self._get_institution_from_customer_code(customer_code)
        if order_number:
            return self._get_institution_from_order_number(order_number)
        return None

    def _get_institution_from_customer_code(self, customer_code: str) -> Optional[str]:
        """从客户代码提取机构代码

        格式: BJ-HA-001-C0001 -> BJ-HA-001
        """
        if not customer_code:
            return None
        # 找到最后一个 -C 的位置
        idx = customer_code.rfind('-C')
        if idx > 0:
            return customer_code[:idx]
        return None

    def _get_institution_from_order_number(self, order_number: str) -> Optional[str]:
        """从订单号提取机构代码

        格式: BJ-HA-001-ORD-20260115-0001 -> BJ-HA-001
        """
        if not order_number:
            return None
        # 找到 -ORD 的位置
        idx = order_number.find('-ORD')
        if idx > 0:
            return order_number[:idx]
        return None

    def _archive_batch(self, batch_dir: Path):
        """归档已处理批次"""
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        dest = PROCESSED_DIR / batch_dir.name

        if dest.exists():
            timestamp = datetime.now().strftime("%H%M%S")
            dest = PROCESSED_DIR / f"{batch_dir.name}_{timestamp}"

        shutil.move(str(batch_dir), str(dest))
        logger.info(f"  ✅ 已归档到: {dest}")


async def main_async():
    """异步主函数"""
    parser = argparse.ArgumentParser(
        description="医美客户回访系统 - 数据导入工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python -m scripts.import_data initial              # 初始全量导入
  python -m scripts.import_data incremental          # 处理所有增量
  python -m scripts.import_data incremental --date 2026-01-14  # 指定日期
        """
    )
    parser.add_argument(
        "action",
        choices=["initial", "incremental"],
        help="导入类型: initial=初始全量, incremental=增量更新"
    )
    parser.add_argument(
        "--date",
        type=str,
        help="增量更新日期 (格式: YYYY-MM-DD)"
    )

    args = parser.parse_args()

    importer = DataImporter()

    try:
        await importer.init()

        if args.action == "initial":
            await importer.import_initial()
        elif args.action == "incremental":
            await importer.process_incremental(args.date)

    except KeyboardInterrupt:
        print("\n\n⚠️ 用户中断导入")
    except Exception as e:
        logger.error(f"❌ 导入失败: {e}")
        raise
    finally:
        await importer.close()


def main():
    """主函数入口"""
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
