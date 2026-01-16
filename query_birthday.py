#!/usr/bin/env python3
"""查询7天内生日的客户"""
import asyncio
from database.postgres.connection import PostgreSQLConnection
from datetime import date

async def main():
    await PostgreSQLConnection.create_pool()
    today = date.today()
    print(f"今天日期: {today}")

    async with PostgreSQLConnection.get_connection() as conn:
        # 查所有客户
        rows = await conn.fetch('''
            SELECT 
                ic.customer_code,
                np.name,
                np.phone,
                np.birthday,
                ic.vip_level,
                ic.total_consumption
            FROM institution_customer_bj_ha_001 ic
            JOIN natural_person np ON ic.person_id = np.person_id
            WHERE np.birthday IS NOT NULL
        ''')

        print(f"共 {len(rows)} 位客户")
        print("="*60)

        birthday_soon = []
        for row in rows:
            birthday = row['birthday']
            this_year_bd = birthday.replace(year=today.year)
            if this_year_bd < today:
                this_year_bd = birthday.replace(year=today.year + 1)
            days = (this_year_bd - today).days

            print(f"{row['name']} | 生日: {birthday.strftime('%m-%d')} | {days}天后生日")

            if days <= 7:
                birthday_soon.append({
                    'customer_code': row['customer_code'],
                    'name': row['name'],
                    'phone': row['phone'],
                    'birthday': birthday,
                    'days': days,
                    'vip_level': row['vip_level'],
                    'total_consumption': row['total_consumption']
                })

        print("="*60)
        if birthday_soon:
            print(f"\n*** 7天内生日的客户 ({len(birthday_soon)}位) ***\n")
            for c in sorted(birthday_soon, key=lambda x: x['days']):
                print(f"  客户代码: {c['customer_code']}")
                print(f"  姓名: {c['name']}")
                print(f"  电话: {c['phone']}")
                print(f"  生日: {c['birthday'].strftime('%m月%d日')}")
                print(f"  距离生日: {c['days']}天")
                print(f"  VIP等级: {c['vip_level']}")
                print(f"  累计消费: ¥{c['total_consumption']:,.2f}")
                print("-"*40)
        else:
            print("\n未来7天内没有客户过生日")

    await PostgreSQLConnection.close_pool()

if __name__ == "__main__":
    asyncio.run(main())

