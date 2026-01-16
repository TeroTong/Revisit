"""
数据导入服务
"""
import json
import aiofiles
from datetime import datetime, date
from typing import Dict, List
from pathlib import Path
import logging

from config.settings import settings
from database.postgres.connection import get_connection

logger = logging.getLogger(__name__)


class DataImporter:
    """数据导入服务"""

    @staticmethod
    async def import_from_json(file_path: str, table_name: str, batch_size: int = 100):
        """从JSON文件导入数据到指定表"""

        try:
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                content = await f.read()
                data = json.loads(content)

            if not isinstance(data, list):
                data = [data]

            logger.info(f"开始导入 {len(data)} 条数据到 {table_name}")

            # 分批导入
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                await DataImporter._import_batch(table_name, batch)

                logger.info(f"已导入 {min(i + batch_size, len(data))}/{len(data)} 条数据")

            logger.info(f"✅ 数据导入完成: {table_name}")

        except FileNotFoundError:
            logger.error(f"文件不存在: {file_path}")
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析失败: {e}")
        except Exception as e:
            logger.error(f"导入数据失败: {e}")
            raise

    @staticmethod
    async def _import_batch(table_name: str, batch: List[Dict]):
        """导入一批数据"""

        if not batch:
            return

        async with get_connection() as conn:
            # 获取列名
            columns = list(batch[0].keys())

            # 构建插入语句
            placeholders = ', '.join([f'${i + 1}' for i in range(len(columns))])
            columns_str = ', '.join(columns)

            query = f'''
                INSERT INTO {table_name} ({columns_str})
                VALUES ({placeholders})
                ON CONFLICT DO NOTHING
            '''

            # 执行批量插入
            values = [
                tuple(
                    DataImporter._convert_value(record.get(col))
                    for col in columns
                )
                for record in batch
            ]

            await conn.executemany(query, values)

    @staticmethod
    def _convert_value(value):
        """转换值类型"""
        if value is None:
            return None
        elif isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        elif isinstance(value, (datetime, date)):
            return value.isoformat()
        return value

    @staticmethod
    async def import_natural_persons(file_path: str):
        """导入自然人客户"""
        await DataImporter.import_from_json(file_path, 'natural_person')

    @staticmethod
    async def import_institutions(file_path: str):
        """导入机构数据"""
        await DataImporter.import_from_json(file_path, 'institution')

    @staticmethod
    async def import_projects(file_path: str):
        """导入项目数据"""
        await DataImporter.import_from_json(file_path, 'project')

    @staticmethod
    async def import_products(file_path: str):
        """导入产品数据"""
        await DataImporter.import_from_json(file_path, 'product')

    @staticmethod
    async def import_doctors(file_path: str):
        """导入医生数据"""
        await DataImporter.import_from_json(file_path, 'doctor')

    @staticmethod
    async def import_institution_data(institution_code: str, data_type: str, file_path: str):
        """导入机构特定数据"""

        suffix = institution_code.lower().replace("-", "_")
        table_name = f"{data_type}_{suffix}"

        await DataImporter.import_from_json(file_path, table_name)

    @staticmethod
    async def update_birthday_reminders(institution_code: str, days_ahead: int = None):
        """更新生日回访任务"""

        if days_ahead is None:
            days_ahead = settings.APP.BIRTHDAY_REMINDER_DAYS_AHEAD

        suffix = institution_code.lower().replace("-", "_")

        async with get_connection() as conn:
            # 删除旧的待回访记录
            await conn.execute(f'''
                DELETE FROM pending_birthday_reminder_{suffix}
                WHERE reminder_date < CURRENT_DATE - INTERVAL '30 days'
            ''')

            # 添加新的生日回访任务
            query = f'''
                INSERT INTO pending_birthday_reminder_{suffix} 
                (institution_customer_id, natural_person_id, birthday, reminder_date, status)
                SELECT 
                    ic.id,
                    np.id,
                    np.birthday,
                    DATE(np.birthday + (EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM np.birthday)) * INTERVAL '1 year')
                        - INTERVAL '{days_ahead} days' as reminder_date,
                    'pending'
                FROM institution_customer_{suffix} ic
                JOIN natural_person np ON ic.natural_person_id = np.id
                WHERE np.birthday IS NOT NULL
                AND np.is_active = TRUE
                AND ic.is_vip = TRUE
                AND NOT EXISTS (
                    SELECT 1 FROM pending_birthday_reminder_{suffix} pbr
                    WHERE pbr.institution_customer_id = ic.id
                    AND pbr.reminder_date = DATE(np.birthday + (EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM np.birthday)) * INTERVAL '1 year')
                        - INTERVAL '{days_ahead} days'
                )
                ON CONFLICT (institution_customer_id) DO UPDATE SET
                    updated_at = CURRENT_TIMESTAMP,
                    status = 'pending'
            '''

            result = await conn.execute(query)
            logger.info(f"已更新生日回访任务: {result}")

    @staticmethod
    async def import_sample_data():
        """导入示例数据"""
        data_dir = Path(settings.APP.DATA_DIR) / "samples"

        if not data_dir.exists():
            logger.warning(f"示例数据目录不存在: {data_dir}")
            return

        # 导入基础数据
        sample_files = [
            ("natural_person", "natural_persons.json"),
            ("institution", "institutions.json"),
            ("project", "projects.json"),
            ("product", "products.json"),
            ("doctor", "doctors.json"),
        ]

        for table_name, filename in sample_files:
            file_path = data_dir / filename
            if file_path.exists():
                await DataImporter.import_from_json(str(file_path), table_name)
            else:
                logger.warning(f"示例文件不存在: {file_path}")

        # 为每个机构导入机构特定数据
        for institution_code in settings.APP.INSTITUTIONS:
            institution_files = [
                ("institution_customer", "institution_customers.json"),
                ("online_consultation", "online_consultations.json"),
                ("offline_consultation", "offline_consultations.json"),
                ("consumption", "consumptions.json"),
            ]

            for data_type, filename in institution_files:
                file_path = data_dir / filename
                if file_path.exists():
                    await DataImporter.import_institution_data(institution_code, data_type, str(file_path))
                else:
                    logger.warning(f"机构示例文件不存在: {file_path}")