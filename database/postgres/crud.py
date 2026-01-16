"""
PostgreSQL CRUD操作
"""
from typing import TypeVar, Generic, Optional, List, Dict, Any

from database.postgres.connection import get_connection

ModelType = TypeVar("ModelType")
CreateSchemaType = TypeVar("CreateSchemaType")
UpdateSchemaType = TypeVar("UpdateSchemaType")


class CRUDBase(Generic[ModelType, CreateSchemaType, UpdateSchemaType]):
    """CRUD基础类"""

    def __init__(self, table_name: str):
        self.table_name = table_name

    async def create(self, data: CreateSchemaType) -> Optional[ModelType]:
        """创建记录"""
        async with get_connection() as conn:
            columns = list(data.keys())
            placeholders = ', '.join([f'${i + 1}' for i in range(len(columns))])
            columns_str = ', '.join(columns)

            query = f'''
                INSERT INTO {self.table_name} ({columns_str})
                VALUES ({placeholders})
                RETURNING *
            '''

            values = list(data.values())
            result = await conn.fetchrow(query, *values)

            return dict(result) if result else None

    async def get(self, id: int) -> Optional[ModelType]:
        """根据ID获取记录"""
        async with get_connection() as conn:
            query = f'''
                SELECT * FROM {self.table_name}
                WHERE id = $1
            '''
            result = await conn.fetchrow(query, id)
            return dict(result) if result else None

    async def get_by_field(self, field: str, value: Any) -> Optional[ModelType]:
        """根据字段获取记录"""
        async with get_connection() as conn:
            query = f'''
                SELECT * FROM {self.table_name}
                WHERE {field} = $1
                LIMIT 1
            '''
            result = await conn.fetchrow(query, value)
            return dict(result) if result else None

    async def get_multi(
            self,
            skip: int = 0,
            limit: int = 100,
            filters: Optional[Dict] = None
    ) -> List[ModelType]:
        """获取多条记录"""
        async with get_connection() as conn:
            where_clause = ""
            values = []

            if filters:
                conditions = []
                for i, (key, value) in enumerate(filters.items(), 1):
                    conditions.append(f"{key} = ${i}")
                    values.append(value)

                if conditions:
                    where_clause = "WHERE " + " AND ".join(conditions)

            query = f'''
                SELECT * FROM {self.table_name}
                {where_clause}
                ORDER BY id
                OFFSET ${len(values) + 1} LIMIT ${len(values) + 2}
            '''

            values.extend([skip, limit])
            results = await conn.fetch(query, *values)
            return [dict(row) for row in results]

    async def update(
            self,
            id: int,
            data: UpdateSchemaType
    ) -> Optional[ModelType]:
        """更新记录"""
        async with get_connection() as conn:
            set_clause = ', '.join([
                f"{key} = ${i + 2}"
                for i, key in enumerate(data.keys())
            ])

            query = f'''
                UPDATE {self.table_name}
                SET {set_clause}
                WHERE id = $1
                RETURNING *
            '''

            values = [id] + list(data.values())
            result = await conn.fetchrow(query, *values)
            return dict(result) if result else None

    async def delete(self, id: int) -> bool:
        """删除记录"""
        async with get_connection() as conn:
            query = f'''
                DELETE FROM {self.table_name}
                WHERE id = $1
            '''
            result = await conn.execute(query, id)
            return "DELETE 1" in result

    async def count(self, filters: Optional[Dict] = None) -> int:
        """统计记录数"""
        async with get_connection() as conn:
            where_clause = ""
            values = []

            if filters:
                conditions = []
                for i, (key, value) in enumerate(filters.items(), 1):
                    conditions.append(f"{key} = ${i}")
                    values.append(value)

                if conditions:
                    where_clause = "WHERE " + " AND ".join(conditions)

            query = f'''
                SELECT COUNT(*) FROM {self.table_name}
                {where_clause}
            '''

            count = await conn.fetchval(query, *values)
            return count if count else 0


class NaturalPersonCRUD(CRUDBase):
    """自然人CRUD操作"""

    def __init__(self):
        super().__init__("natural_person")

    async def get_by_phone(self, phone: str) -> Optional[Dict]:
        """根据手机号获取自然人"""
        return await self.get_by_field("phone", phone)

    async def get_by_birthday_month(self, month: int) -> List[Dict]:
        """获取生日在指定月份的自然人"""
        async with get_connection() as conn:
            query = '''
                SELECT * FROM natural_person
                WHERE EXTRACT(MONTH FROM birthday) = $1
                AND is_active = TRUE
            '''
            results = await conn.fetch(query, month)
            return [dict(row) for row in results]

    async def get_vip_customers(self, institution_code: str) -> List[Dict]:
        """获取机构的VIP客户"""
        suffix = institution_code.lower().replace("-", "_")
        table_name = f"institution_customer_{suffix}"

        async with get_connection() as conn:
            query = f'''
                SELECT np.*
                FROM natural_person np
                JOIN {table_name} ic ON np.id = ic.natural_person_id
                WHERE ic.is_vip = TRUE
                AND np.is_active = TRUE
            '''
            results = await conn.fetch(query)
            return [dict(row) for row in results]


class InstitutionCRUD(CRUDBase):
    """机构CRUD操作"""

    def __init__(self):
        super().__init__("institution")

    async def get_by_code(self, institution_code: str) -> Optional[Dict]:
        """根据机构代码获取机构"""
        return await self.get_by_field("institution_code", institution_code)

    async def get_active_institutions(self) -> List[Dict]:
        """获取所有活跃机构"""
        return await self.get_multi(filters={"is_active": True})


class ProjectCRUD(CRUDBase):
    """项目CRUD操作"""

    def __init__(self):
        super().__init__("project")

    async def get_by_category(self, category: str) -> List[Dict]:
        """根据类别获取项目"""
        return await self.get_multi(filters={"category": category})

    async def search_by_name(self, name: str) -> List[Dict]:
        """根据名称搜索项目"""
        async with get_connection() as conn:
            query = '''
                SELECT * FROM project
                WHERE name ILIKE $1
                AND is_active = TRUE
                LIMIT 10
            '''
            results = await conn.fetch(query, f"%{name}%")
            return [dict(row) for row in results]


class ProductCRUD(CRUDBase):
    """产品CRUD操作"""

    def __init__(self):
        super().__init__("product")

    async def get_by_brand(self, brand: str) -> List[Dict]:
        """根据品牌获取产品"""
        return await self.get_multi(filters={"brand": brand})


class DoctorCRUD(CRUDBase):
    """医生CRUD操作"""

    def __init__(self):
        super().__init__("doctor")

    async def get_by_institution(self, institution_id: int) -> List[Dict]:
        """根据机构获取医生"""
        return await self.get_multi(filters={"institution_id": institution_id})

    async def get_by_specialty(self, specialty: str) -> List[Dict]:
        """根据专长获取医生"""
        async with get_connection() as conn:
            query = '''
                SELECT * FROM doctor
                WHERE specialty ILIKE $1
                AND is_active = TRUE
            '''
            results = await conn.fetch(query, f"%{specialty}%")
            return [dict(row) for row in results]