"""
回访管理API路由
"""
from typing import List, Optional, Dict, Any
from datetime import date, timedelta
from fastapi import APIRouter, HTTPException, Query, Path, BackgroundTasks
from pydantic import BaseModel

from services.birthday_reminder import BirthdayReminderService
from services.data_sync import DataSyncService

router = APIRouter(prefix="/reminders", tags=["回访管理"])


# ==================== 响应模型 ====================

class CustomerBirthdayInfo(BaseModel):
    """客户生日信息"""
    customer_code: str
    name: str
    phone_suffix: str  # 只显示后4位
    vip_level: str
    total_consumption: float
    days_until_birthday: int


class BirthdayReportResponse(BaseModel):
    """生日报告响应"""
    institution_code: str
    report_date: str
    days_ahead: int
    total_customers: int
    vip_customers: int
    by_day: Dict[str, Any]


class ReminderRunResponse(BaseModel):
    """回访运行响应"""
    message: str
    institution_code: str
    success_count: int
    fail_count: int


class GenerateContentRequest(BaseModel):
    """生成内容请求"""
    customer_code: str


class BatchGenerateContentRequest(BaseModel):
    """批量生成内容请求"""
    customer_codes: List[str]  # 客户代码列表


class UpdateReminderStatusRequest(BaseModel):
    """更新回访状态请求"""
    customer_code: str
    status: str  # PENDING, DEFERRED, COMPLETED


class GenerateContentResponse(BaseModel):
    """生成内容响应"""
    customer_code: str
    customer_name: str
    content: str


class BatchGenerateContentResponse(BaseModel):
    """批量生成内容响应"""
    total: int
    success_count: int
    fail_count: int
    results: List[Dict[str, Any]]  # 每个客户的生成结果


# ==================== API 路由 ====================

@router.get("/{institution_code}/upcoming-birthdays", response_model=BirthdayReportResponse)
async def get_upcoming_birthdays(
    institution_code: str = Path(..., description="机构代码"),
    days: int = Query(7, ge=1, le=30, description="查询天数")
):
    """获取即将生日的客户报告

    返回未来N天内生日的客户列表，按天分组。
    """
    try:
        service = BirthdayReminderService()
        report = await service.get_upcoming_birthdays_report(institution_code, days)
        return BirthdayReportResponse(**report)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取生日报告失败: {str(e)}")


@router.get("/{institution_code}/upcoming-birthdays-paged")
async def get_upcoming_birthdays_paged(
    institution_code: str = Path(..., description="机构代码"),
    days: int = Query(7, ge=1, le=365, description="查询天数"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页条数"),
    status: str = Query(None, description="状态筛选: PENDING, DEFERRED, COMPLETED"),
    search: str = Query(None, description="搜索关键词")
):
    """分页获取即将生日的客户（适用于大数据量）

    支持：
    - 分页查询
    - 状态筛选
    - 关键词搜索
    """
    from database.postgres.connection import PostgreSQLConnection
    from datetime import date, timedelta

    try:
        await PostgreSQLConnection.create_pool()

        suffix = institution_code.lower().replace('-', '_')
        customer_table = f"institution_customer_{suffix}"
        reminder_table = f"birthday_reminder_{suffix}"
        today = date.today()

        async with PostgreSQLConnection.get_connection() as conn:
            # 构建日期条件
            date_conditions = []
            for i in range(days + 1):
                target_date = today + timedelta(days=i)
                date_conditions.append(f"(EXTRACT(MONTH FROM np.birthday) = {target_date.month} AND EXTRACT(DAY FROM np.birthday) = {target_date.day})")

            date_where = " OR ".join(date_conditions)

            # 基础查询
            base_query = f'''
                SELECT 
                    ic.customer_code,
                    ic.vip_level,
                    ic.total_consumption,
                    np.name,
                    np.phone,
                    np.birthday,
                    COALESCE(br.reminder_status, 'PENDING') as reminder_status
                FROM {customer_table} ic
                JOIN natural_person np ON ic.person_id = np.person_id
                LEFT JOIN {reminder_table} br ON ic.institution_customer_id = br.institution_customer_id 
                    AND br.reminder_date = $1
                WHERE ic.status = 'ACTIVE'
                AND np.birthday IS NOT NULL
                AND ({date_where})
            '''

            # 添加状态筛选
            params = [today]
            if status:
                base_query += f" AND COALESCE(br.reminder_status, 'PENDING') = ${len(params) + 1}"
                params.append(status)

            # 添加搜索条件
            if search:
                search_pattern = f"%{search}%"
                base_query += f" AND (np.name ILIKE ${len(params) + 1} OR ic.customer_code ILIKE ${len(params) + 1} OR np.phone LIKE ${len(params) + 1})"
                params.append(search_pattern)

            # 统计总数
            count_query = f"SELECT COUNT(*) FROM ({base_query}) sub"
            total_count = await conn.fetchval(count_query, *params)

            # 统计各状态数量
            stats_query = f'''
                SELECT 
                    COALESCE(br.reminder_status, 'PENDING') as status,
                    COUNT(*) as count
                FROM {customer_table} ic
                JOIN natural_person np ON ic.person_id = np.person_id
                LEFT JOIN {reminder_table} br ON ic.institution_customer_id = br.institution_customer_id 
                    AND br.reminder_date = $1
                WHERE ic.status = 'ACTIVE'
                AND np.birthday IS NOT NULL
                AND ({date_where})
                GROUP BY COALESCE(br.reminder_status, 'PENDING')
            '''
            stats_rows = await conn.fetch(stats_query, today)
            status_counts = {row['status']: row['count'] for row in stats_rows}

            # 分页查询
            offset = (page - 1) * page_size
            paged_query = base_query + f'''
                ORDER BY 
                    CASE COALESCE(br.reminder_status, 'PENDING')
                        WHEN 'PENDING' THEN 0
                        WHEN 'DEFERRED' THEN 1
                        WHEN 'COMPLETED' THEN 2
                    END,
                    EXTRACT(MONTH FROM np.birthday),
                    EXTRACT(DAY FROM np.birthday)
                LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}
            '''
            params.extend([page_size, offset])

            rows = await conn.fetch(paged_query, *params)

            # 计算距离生日天数
            customers = []
            for row in rows:
                birthday = row['birthday']
                this_year_bd = birthday.replace(year=today.year)
                if this_year_bd < today:
                    this_year_bd = birthday.replace(year=today.year + 1)
                days_until = (this_year_bd - today).days

                customers.append({
                    'customer_code': row['customer_code'],
                    'name': row['name'],
                    'phone_suffix': row['phone'][-4:] if row['phone'] else '',
                    'vip_level': row['vip_level'],
                    'total_consumption': float(row['total_consumption'] or 0),
                    'days_until_birthday': days_until,
                    'status': row['reminder_status']
                })

            return {
                'institution_code': institution_code,
                'report_date': today.isoformat(),
                'days_ahead': days,
                'total_count': total_count,
                'status_counts': {
                    'PENDING': status_counts.get('PENDING', 0),
                    'DEFERRED': status_counts.get('DEFERRED', 0),
                    'COMPLETED': status_counts.get('COMPLETED', 0)
                },
                'page': page,
                'page_size': page_size,
                'total_pages': (total_count + page_size - 1) // page_size,
                'customers': customers
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"分页查询失败: {str(e)}")
    finally:
        await PostgreSQLConnection.close_pool()


@router.get("/{institution_code}/today-birthdays")
async def get_today_birthdays(
    institution_code: str = Path(..., description="机构代码")
):
    """获取今天生日的客户"""
    try:
        service = BirthdayReminderService()
        customers = await service.get_today_birthday_customers(institution_code)

        return {
            "institution_code": institution_code,
            "date": date.today().isoformat(),
            "count": len(customers),
            "customers": [
                {
                    "customer_code": c.get("customer_code"),
                    "name": c.get("name"),
                    "vip_level": c.get("vip_level"),
                    "phone_suffix": c.get("phone", "")[-4:] if c.get("phone") else ""
                }
                for c in customers
            ]
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取今日生日客户失败: {str(e)}")


@router.post("/{institution_code}/run-birthday-reminders", response_model=ReminderRunResponse)
async def run_birthday_reminders(
    institution_code: str = Path(..., description="机构代码"),
    background_tasks: BackgroundTasks = None
):
    """运行生日回访任务

    为指定机构运行生日回访任务，生成个性化内容并发送通知。
    """
    try:
        service = BirthdayReminderService()

        # 在后台运行
        if background_tasks:
            background_tasks.add_task(service.run_daily_reminders, institution_code)
            return ReminderRunResponse(
                message="生日回访任务已在后台启动",
                institution_code=institution_code,
                success_count=0,
                fail_count=0
            )
        else:
            await service.run_daily_reminders(institution_code)
            return ReminderRunResponse(
                message="生日回访任务执行完成",
                institution_code=institution_code,
                success_count=0,  # TODO: 从 service 获取实际统计
                fail_count=0
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"运行回访任务失败: {str(e)}")


@router.post("/{institution_code}/update-reminder-status")
async def update_reminder_status(
    institution_code: str = Path(..., description="机构代码"),
    request: UpdateReminderStatusRequest = None
):
    """更新客户回访状态

    状态值：
    - PENDING: 待回访
    - DEFERRED: 暂缓回访
    - COMPLETED: 已完成回访
    """
    from database.postgres.connection import PostgreSQLConnection
    from datetime import date

    valid_statuses = ['PENDING', 'DEFERRED', 'COMPLETED']
    if request.status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"无效的状态值，必须是: {valid_statuses}")

    try:
        await PostgreSQLConnection.create_pool()

        suffix = institution_code.lower().replace('-', '_')
        customer_table = f"institution_customer_{suffix}"
        reminder_table = f"birthday_reminder_{suffix}"

        async with PostgreSQLConnection.get_connection() as conn:
            # 获取客户信息
            customer = await conn.fetchrow(f'''
                SELECT ic.institution_customer_id, ic.institution_id, np.birthday
                FROM {customer_table} ic
                JOIN natural_person np ON ic.person_id = np.person_id
                WHERE ic.customer_code = $1
            ''', request.customer_code)

            if not customer:
                raise HTTPException(status_code=404, detail=f"客户不存在: {request.customer_code}")

            today = date.today()

            # 更新或插入回访记录
            result = await conn.fetchrow(f'''
                INSERT INTO {reminder_table} 
                    (institution_id, institution_customer_id, birth_month, birth_day, 
                     reminder_type, reminder_date, reminder_status, complete_date)
                VALUES ($1, $2, $3, $4, 'BIRTHDAY', $5, $6, $7)
                ON CONFLICT (institution_id, institution_customer_id, reminder_date)
                DO UPDATE SET 
                    reminder_status = EXCLUDED.reminder_status,
                    complete_date = EXCLUDED.complete_date,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING birthday_reminder_id, reminder_status
            ''',
                customer['institution_id'],
                customer['institution_customer_id'],
                customer['birthday'].month if customer['birthday'] else today.month,
                customer['birthday'].day if customer['birthday'] else today.day,
                today,
                request.status,
                today if request.status == 'COMPLETED' else None
            )

            return {
                "success": True,
                "customer_code": request.customer_code,
                "status": request.status,
                "message": f"回访状态已更新为: {request.status}"
            }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新回访状态失败: {str(e)}")
    finally:
        await PostgreSQLConnection.close_pool()


@router.get("/{institution_code}/reminder-statuses")
async def get_reminder_statuses(
    institution_code: str = Path(..., description="机构代码"),
    customer_codes: str = Query(None, description="客户代码列表，逗号分隔")
):
    """获取客户的回访状态"""
    from database.postgres.connection import PostgreSQLConnection
    from datetime import date

    try:
        await PostgreSQLConnection.create_pool()

        suffix = institution_code.lower().replace('-', '_')
        customer_table = f"institution_customer_{suffix}"
        reminder_table = f"birthday_reminder_{suffix}"
        today = date.today()

        async with PostgreSQLConnection.get_connection() as conn:
            # 检查表是否存在
            table_exists = await conn.fetchval(f'''
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = $1
                )
            ''', reminder_table)

            if not table_exists:
                return {"statuses": {}}

            # 构建查询
            if customer_codes:
                codes = [c.strip() for c in customer_codes.split(',')]
                rows = await conn.fetch(f'''
                    SELECT ic.customer_code, br.reminder_status
                    FROM {reminder_table} br
                    JOIN {customer_table} ic ON br.institution_customer_id = ic.institution_customer_id
                    WHERE ic.customer_code = ANY($1)
                    AND br.reminder_date = $2
                ''', codes, today)
            else:
                rows = await conn.fetch(f'''
                    SELECT ic.customer_code, br.reminder_status
                    FROM {reminder_table} br
                    JOIN {customer_table} ic ON br.institution_customer_id = ic.institution_customer_id
                    WHERE br.reminder_date = $2
                ''', today)

            statuses = {row['customer_code']: row['reminder_status'] for row in rows}
            return {"statuses": statuses}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取回访状态失败: {str(e)}")
    finally:
        await PostgreSQLConnection.close_pool()


@router.post("/{institution_code}/generate-content", response_model=GenerateContentResponse)
async def generate_reminder_content(
    institution_code: str = Path(..., description="机构代码"),
    request: GenerateContentRequest = None
):
    """为指定客户生成回访内容

    使用 LLM 根据客户历史生成个性化的生日祝福内容。
    """
    from database.postgres.connection import PostgreSQLConnection

    try:
        service = BirthdayReminderService()

        # 直接使用 PostgreSQL 连接查询客户
        await PostgreSQLConnection.create_pool()

        try:
            suffix = institution_code.lower().replace('-', '_')
            table_name = f"institution_customer_{suffix}"
            order_table = f"consumption_record_{suffix}"
            inst_project_table = f"institution_project_{suffix}"
            inst_product_table = f"institution_product_{suffix}"
            inst_doctor_table = f"institution_doctor_{suffix}"

            async with PostgreSQLConnection.get_connection() as conn:
                # 获取客户信息
                customer_row = await conn.fetchrow(f'''
                    SELECT 
                        ic.institution_customer_id,
                        ic.customer_code,
                        ic.vip_level,
                        ic.status,
                        ic.total_consumption,
                        ic.consumption_count,
                        ic.last_visit_date,
                        np.name,
                        np.phone,
                        np.gender,
                        np.birthday
                    FROM {table_name} ic
                    JOIN natural_person np ON ic.person_id = np.person_id
                    WHERE ic.customer_code = $1
                ''', request.customer_code)

                if not customer_row:
                    raise HTTPException(status_code=404, detail=f"客户不存在: {request.customer_code}")

                customer = dict(customer_row)

                # 获取消费历史
                history_rows = await conn.fetch(f'''
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
                    LIMIT 10
                ''', customer['institution_customer_id'])

                history = [dict(row) for row in history_rows]

            # 生成内容
            content = await service.generate_reminder_content(customer, history)

            return GenerateContentResponse(
                customer_code=request.customer_code,
                customer_name=customer.get("name", "客户"),
                content=content
            )

        finally:
            await PostgreSQLConnection.close_pool()

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"生成内容失败: {str(e)}")


@router.post("/{institution_code}/batch-generate-content", response_model=BatchGenerateContentResponse)
async def batch_generate_reminder_content(
    institution_code: str = Path(..., description="机构代码"),
    request: BatchGenerateContentRequest = None
):
    """批量为多个客户生成回访内容

    使用 LLM 一次性为多个客户生成个性化的生日祝福内容。
    最多支持 20 个客户同时生成。
    """
    from database.postgres.connection import PostgreSQLConnection
    import json

    if not request.customer_codes:
        raise HTTPException(status_code=400, detail="客户代码列表不能为空")

    if len(request.customer_codes) > 20:
        raise HTTPException(status_code=400, detail="一次最多支持 20 个客户")

    try:
        service = BirthdayReminderService()
        await PostgreSQLConnection.create_pool()

        try:
            suffix = institution_code.lower().replace('-', '_')
            table_name = f"institution_customer_{suffix}"
            order_table = f"consumption_record_{suffix}"
            inst_project_table = f"institution_project_{suffix}"
            inst_product_table = f"institution_product_{suffix}"
            inst_doctor_table = f"institution_doctor_{suffix}"

            # 收集所有客户的信息
            all_customers_data = []

            async with PostgreSQLConnection.get_connection() as conn:
                for customer_code in request.customer_codes:
                    # 获取客户信息
                    customer_row = await conn.fetchrow(f'''
                        SELECT 
                            ic.institution_customer_id,
                            ic.customer_code,
                            ic.vip_level,
                            ic.status,
                            ic.total_consumption,
                            ic.consumption_count,
                            ic.last_visit_date,
                            np.name,
                            np.phone,
                            np.gender,
                            np.birthday
                        FROM {table_name} ic
                        JOIN natural_person np ON ic.person_id = np.person_id
                        WHERE ic.customer_code = $1
                    ''', customer_code)

                    if not customer_row:
                        all_customers_data.append({
                            "customer_code": customer_code,
                            "error": "客户不存在"
                        })
                        continue

                    customer = dict(customer_row)

                    # 获取消费历史（简化版，只取最近5条）
                    history_rows = await conn.fetch(f'''
                        SELECT 
                            co.order_date,
                            co.actual_amount,
                            p.name as project_name,
                            pr.name as product_name
                        FROM {order_table} co
                        LEFT JOIN {inst_project_table} ip ON co.institution_project_id = ip.institution_project_id
                        LEFT JOIN project p ON ip.project_id = p.project_id
                        LEFT JOIN {inst_product_table} ipr ON co.institution_product_id = ipr.institution_product_id
                        LEFT JOIN product pr ON ipr.product_id = pr.product_id
                        WHERE co.institution_customer_id = $1
                        ORDER BY co.order_date DESC
                        LIMIT 5
                    ''', customer['institution_customer_id'])

                    history = [dict(row) for row in history_rows]

                    all_customers_data.append({
                        "customer_code": customer_code,
                        "customer": customer,
                        "history": history
                    })

            # 使用 LLM 批量生成话术
            results = await service.batch_generate_reminder_content(all_customers_data)

            success_count = len([r for r in results if r.get("success")])
            fail_count = len(results) - success_count

            return BatchGenerateContentResponse(
                total=len(request.customer_codes),
                success_count=success_count,
                fail_count=fail_count,
                results=results
            )

        finally:
            await PostgreSQLConnection.close_pool()

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"批量生成内容失败: {str(e)}")


@router.get("/{institution_code}/customer/{customer_code}/history")
async def get_customer_history(
    institution_code: str = Path(..., description="机构代码"),
    customer_code: str = Path(..., description="客户代码"),
    limit: int = Query(10, ge=1, le=50, description="记录数量")
):
    """获取客户消费历史"""
    try:
        data_service = DataSyncService()
        await data_service.init_connections()

        try:
            # 先获取客户信息
            customers = await data_service.get_upcoming_birthday_customers(institution_code, days_ahead=365)
            customer = next((c for c in customers if c.get("customer_code") == customer_code), None)

            if not customer:
                raise HTTPException(status_code=404, detail=f"客户不存在: {customer_code}")

            # 获取消费历史
            history = await data_service.get_customer_consumption_history(
                customer.get("institution_customer_id"),
                institution_code,
                limit=limit
            )

            return {
                "customer_code": customer_code,
                "customer_name": customer.get("name"),
                "total_consumption": customer.get("total_consumption", 0),
                "consumption_count": customer.get("consumption_count", 0),
                "history": history
            }

        finally:
            await data_service.close_connections()

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取客户历史失败: {str(e)}")


# ==================== 批量操作 ====================

@router.post("/run-all-institutions")
async def run_all_institutions(background_tasks: BackgroundTasks):
    """为所有机构运行生日回访任务"""
    from config.settings import settings

    async def run_all():
        service = BirthdayReminderService()
        for inst_code in settings.APP.INSTITUTIONS:
            try:
                await service.run_daily_reminders(inst_code)
            except Exception as e:
                pass  # 错误已在 service 中记录

    background_tasks.add_task(run_all)

    return {
        "message": "已启动所有机构的生日回访任务",
        "institutions": settings.APP.INSTITUTIONS
    }
