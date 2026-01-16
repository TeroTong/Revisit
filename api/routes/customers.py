"""
客户管理API路由
"""
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from pydantic import BaseModel

from core.models.customer import Customer
from core.services.data_service import DataService

router = APIRouter(prefix="/customers", tags=["客户管理"])


class CustomerResponse(BaseModel):
    """客户响应模型"""
    id: int
    name: str
    phone: str
    level: str
    total_consumption: float
    last_visit_date: Optional[str]
    is_vip: bool


class CustomerListResponse(BaseModel):
    """客户列表响应"""
    total: int
    customers: List[CustomerResponse]


@router.get("/{institution_code}", response_model=CustomerListResponse)
async def get_customers(
        institution_code: str = Path(..., description="机构代码"),
        skip: int = Query(0, ge=0, description="跳过记录数"),
        limit: int = Query(100, ge=1, le=1000, description="每页记录数"),
        level: Optional[str] = Query(None, description="会员等级"),
        is_vip: Optional[bool] = Query(None, description="是否VIP")
):
    """获取客户列表"""

    try:
        suffix = institution_code.lower().replace("-", "_")
        table_name = f"institution_customer_{suffix}"

        # 构建查询条件
        where_conditions = []
        params = []

        if level:
            where_conditions.append(f"level = ${len(params) + 1}")
            params.append(level)

        if is_vip is not None:
            where_conditions.append(f"is_vip = ${len(params) + 1}")
            params.append(is_vip)

        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)

        from database.postgres.connection import get_connection

        async with get_connection() as conn:
            # 获取总数
            count_query = f"SELECT COUNT(*) FROM {table_name} {where_clause}"
            total = await conn.fetchval(count_query, *params)

            # 获取客户列表
            customers_query = f'''
                SELECT ic.*, np.name, np.phone
                FROM {table_name} ic
                JOIN natural_person np ON ic.natural_person_id = np.id
                {where_clause}
                ORDER BY ic.id
                OFFSET ${len(params) + 1} LIMIT ${len(params) + 2}
            '''

            params.extend([skip, limit])
            rows = await conn.fetch(customers_query, *params)

            customers = []
            for row in rows:
                customers.append(CustomerResponse(
                    id=row['id'],
                    name=row['name'],
                    phone=row['phone'],
                    level=row['level'],
                    total_consumption=float(row['total_consumption']) if row['total_consumption'] else 0.0,
                    last_visit_date=row['last_visit_date'].isoformat() if row['last_visit_date'] else None,
                    is_vip=row['is_vip']
                ))

            return CustomerListResponse(total=total, customers=customers)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取客户列表失败: {str(e)}")


@router.get("/{institution_code}/{customer_id}", response_model=CustomerResponse)
async def get_customer(
        institution_code: str = Path(..., description="机构代码"),
        customer_id: int = Path(..., ge=1, description="客户ID")
):
    """获取客户详情"""

    try:
        customer = await DataService.get_customer_by_id(customer_id, institution_code)

        if not customer:
            raise HTTPException(status_code=404, detail="客户不存在")

        return CustomerResponse(
            id=customer.id,
            name=customer.name,
            phone=customer.phone,
            level=customer.level,
            total_consumption=customer.total_consumption,
            last_visit_date=customer.last_visit_date.isoformat() if customer.last_visit_date else None,
            is_vip=customer.is_vip
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取客户详情失败: {str(e)}")


@router.get("/{institution_code}/birthday/today")
async def get_today_birthday_customers(
        institution_code: str = Path(..., description="机构代码")
):
    """获取今天生日的客户"""

    try:
        customers = await DataService.get_today_birthday_customers(institution_code)

        return {
            "total": len(customers),
            "customers": [
                {
                    "id": customer.id,
                    "name": customer.name,
                    "phone": customer.phone,
                    "level": customer.level,
                    "birthday": customer.birthday.isoformat(),
                    "age": customer.get_age()
                }
                for customer in customers
            ]
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取今日生日客户失败: {str(e)}")


@router.get("/{institution_code}/birthday/upcoming")
async def get_upcoming_birthday_customers(
        institution_code: str = Path(..., description="机构代码"),
        days_ahead: int = Query(7, ge=1, le=30, description="提前天数")
):
    """获取即将生日的客户"""

    try:
        customers = await DataService.get_upcoming_birthday_customers(institution_code, days_ahead)

        return {
            "total": len(customers),
            "days_ahead": days_ahead,
            "customers": [
                {
                    "id": customer.id,
                    "name": customer.name,
                    "phone": customer.phone,
                    "level": customer.level,
                    "birthday": customer.birthday.isoformat(),
                    "days_until_birthday": customer.days_until_birthday(),
                    "age": customer.get_age()
                }
                for customer in customers
            ]
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取即将生日客户失败: {str(e)}")