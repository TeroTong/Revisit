"""
测试数据模型
"""
import pytest
from datetime import date
from core.models.customer import Customer
from core.models.project import Project
from core.models.reminder import Reminder, ReminderType, ReminderStatus, ReminderChannel


class TestCustomerModel:
    """测试客户模型"""

    def test_customer_creation(self):
        """测试客户创建"""
        customer = Customer(
            person_id="test_001",
            name="张三",
            gender="男",
            birthday=date(1990, 1, 1),
            phone="13800138000",
            institution_code="BJ-HA-001",
            customer_code="BJ-HA-001_001"
        )

        assert customer.name == "张三"
        assert customer.gender == "男"
        assert customer.get_age() > 0

    def test_customer_age_calculation(self):
        """测试年龄计算"""
        # 测试生日还没过的情况
        birthday = date(1990, 12, 31)
        customer = Customer(
            person_id="test_002",
            name="李四",
            gender="女",
            birthday=birthday,
            phone="13800138001",
            institution_code="BJ-HA-001",
            customer_code="BJ-HA-001_002"
        )

        # 注意：实际年龄会依赖当前日期
        age = customer.get_age()
        assert isinstance(age, int)
        assert age > 0

    def test_customer_birthday_check(self):
        """测试生日检查"""
        today = date.today()
        customer = Customer(
            person_id="test_003",
            name="王五",
            gender="男",
            birthday=date(today.year, today.month, today.day),
            phone="13800138002",
            institution_code="BJ-HA-001",
            customer_code="BJ-HA-001_003"
        )

        assert customer.is_birthday_today() is True

    def test_customer_to_dict(self):
        """测试转换为字典"""
        customer = Customer(
            person_id="test_004",
            name="赵六",
            gender="女",
            birthday=date(1995, 5, 15),
            phone="13800138003",
            institution_code="BJ-HA-001",
            customer_code="BJ-HA-001_004"
        )

        data = customer.to_dict()
        assert isinstance(data, dict)
        assert data["name"] == "赵六"
        assert "birthday" in data


class TestProjectModel:
    """测试项目模型"""

    def test_project_creation(self):
        """测试项目创建"""
        project = Project(
            project_code="PROJ_001",
            name="激光祛斑",
            category="皮肤护理",
            price=2999.00
        )

        assert project.name == "激光祛斑"
        assert project.category == "皮肤护理"
        assert project.price == 2999.00

    def test_project_price_validation(self):
        """测试价格验证"""
        with pytest.raises(ValueError):
            Project(
                project_code="PROJ_002",
                name="测试项目",
                category="测试",
                price=-100.00
            )


class TestReminderModel:
    """测试回访模型"""

    def test_reminder_creation(self):
        """测试回访创建"""
        reminder = Reminder(
            reminder_type=ReminderType.BIRTHDAY,
            institution_code="BJ-HA-001",
            customer_id=1,
            customer_code="BJ-HA-001_001",
            reminder_date=date.today(),
            channel=ReminderChannel.WECHAT,
            status=ReminderStatus.PENDING
        )

        assert reminder.reminder_type == ReminderType.BIRTHDAY
        assert reminder.status == ReminderStatus.PENDING
        assert reminder.channel == ReminderChannel.WECHAT

    def test_reminder_status_transitions(self):
        """测试回访状态转换"""
        reminder = Reminder(
            reminder_type=ReminderType.BIRTHDAY,
            institution_code="BJ-HA-001",
            customer_id=1,
            customer_code="BJ-HA-001_001",
            reminder_date=date.today(),
            channel=ReminderChannel.WECHAT,
            status=ReminderStatus.PENDING
        )

        # 标记为处理中
        reminder.mark_processing()
        assert reminder.status == ReminderStatus.PROCESSING

        # 标记为完成
        reminder.mark_completed(content="测试内容", response="客户回复")
        assert reminder.status == ReminderStatus.COMPLETED
        assert reminder.content == "测试内容"
        assert reminder.response == "客户回复"

        # 标记为失败
        reminder.mark_failed("测试错误")
        assert reminder.status == ReminderStatus.FAILED
        assert reminder.error_message == "测试错误"

    def test_reminder_priority_validation(self):
        """测试优先级验证"""
        with pytest.raises(ValueError):
            Reminder(
                reminder_type=ReminderType.BIRTHDAY,
                institution_code="BJ-HA-001",
                customer_id=1,
                customer_code="BJ-HA-001_001",
                reminder_date=date.today(),
                channel=ReminderChannel.WECHAT,
                status=ReminderStatus.PENDING,
                priority=0  # 无效的优先级
            )


if __name__ == "__main__":
    pytest.main([__file__])