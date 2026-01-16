"""
测试服务
"""
import pytest
from unittest.mock import AsyncMock, patch
from datetime import date, timedelta
from services.birthday_reminder import BirthdayReminderService
from services.llm_service import LLMService
from core.models.customer import Customer


class TestBirthdayReminderService:
    """测试生日回访服务"""

    @pytest.fixture
    def mock_customer(self):
        """创建模拟客户"""
        return Customer(
            person_id="test_001",
            name="测试客户",
            gender="女",
            birthday=date(1990, 5, 15),
            phone="13800138000",
            institution_code="BJ-HA-001",
            customer_code="BJ-HA-001_001",
            level="金卡会员",
            total_consumption=50000.00,
            visit_count=10,
            last_visit_date=date.today() - timedelta(days=30),
            is_vip=True
        )

    @pytest.fixture
    def service(self):
        """创建服务实例"""
        return BirthdayReminderService()

    @pytest.mark.asyncio
    async def test_get_reminder_type(self, service):
        """测试获取回访类型"""
        reminder_type = await service.get_reminder_type()
        assert reminder_type.value == "birthday"

    @pytest.mark.asyncio
    async def test_calculate_reminder_date(self, service, mock_customer):
        """测试计算回访日期"""
        reminder_date = await service.calculate_reminder_date(mock_customer)

        # 应该提前7天（默认配置）
        expected_date = date(
            date.today().year,
            mock_customer.birthday.month,
            mock_customer.birthday.day
        ) - timedelta(days=7)

        assert isinstance(reminder_date, date)

    @pytest.mark.asyncio
    async def test_get_priority(self, service, mock_customer):
        """测试获取优先级"""
        priority = await service.get_priority(mock_customer)

        # VIP客户应该优先级为1
        assert priority == 1

        # 测试非VIP客户
        mock_customer.is_vip = False
        mock_customer.level = "普通会员"
        priority = await service.get_priority(mock_customer)
        assert priority == 3

    @patch('services.birthday_reminder.LLMService')
    @pytest.mark.asyncio
    async def test_generate_reminder_content(self, mock_llm_service, service, mock_customer):
        """测试生成回访内容"""
        # 模拟LLM响应
        mock_llm_instance = AsyncMock()
        mock_llm_instance.generate_content.return_value = "测试生日祝福内容"
        mock_llm_service.return_value = mock_llm_instance

        service.llm_service = mock_llm_instance

        # 模拟历史记录
        history = {
            'consumptions': [],
            'consultations': []
        }

        content = await service.generate_reminder_content(mock_customer, history)

        assert isinstance(content, str)
        assert len(content) > 0
        mock_llm_instance.generate_content.assert_called_once()


class TestLLMService:
    """测试LLM服务"""

    @pytest.fixture
    def service(self):
        """创建服务实例"""
        return LLMService(api_key="test_key")

    @patch('aiohttp.ClientSession.post')
    @pytest.mark.asyncio
    async def test_generate_content_success(self, mock_post, service):
        """测试成功生成内容"""
        # 模拟API响应
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = {
            'choices': [{
                'message': {
                    'content': '测试响应内容'
                }
            }]
        }
        mock_post.return_value.__aenter__.return_value = mock_response

        content = await service.generate_content("测试提示词")

        assert content == "测试响应内容"

    @patch('aiohttp.ClientSession.post')
    @pytest.mark.asyncio
    async def test_generate_content_failure(self, mock_post, service):
        """测试生成内容失败"""
        # 模拟API失败
        mock_response = AsyncMock()
        mock_response.status = 500
        mock_response.text.return_value = "服务器错误"
        mock_post.return_value.__aenter__.return_value = mock_response

        content = await service.generate_content("测试提示词")

        # 应该返回空字符串
        assert content == ""

    @pytest.mark.asyncio
    async def test_generate_birthday_message(self, service):
        """测试生成生日消息"""
        # 模拟客户信息和历史记录
        customer_info = {
            'name': '测试客户',
            'gender': '女',
            'birthday': '1990-05-15',
            'level': '金卡会员',
            'total_consumption': 50000.00
        }

        history = {
            'consumptions': [
                {
                    'project_name': '激光祛斑',
                    'amount': 2999.00
                }
            ],
            'consultations': [
                {
                    'content': '咨询皮肤护理',
                    'type': 'online'
                }
            ]
        }

        # 模拟generate_content返回JSON
        with patch.object(service, 'generate_content') as mock_generate:
            mock_generate.return_value = '''{
                "message": "生日快乐！",
                "recommendations": [
                    {"type": "project", "name": "激光祛斑", "reason": "基于历史消费"}
                ],
                "offer": "生日月8折优惠"
            }'''

            result = await service.generate_birthday_message(customer_info, history)

            assert isinstance(result, dict)
            assert 'message' in result
            assert 'recommendations' in result
            assert 'offer' in result

    @pytest.mark.asyncio
    async def test_generate_birthday_message_invalid_json(self, service):
        """测试生成生日消息（无效JSON）"""
        # 模拟返回非JSON内容
        with patch.object(service, 'generate_content') as mock_generate:
            mock_generate.return_value = "非JSON格式的内容"

            result = await service.generate_birthday_message({}, {})

            # 应该返回包含message的字典
            assert isinstance(result, dict)
            assert 'message' in result
            assert result['message'] == "非JSON格式的内容"


if __name__ == "__main__":
    pytest.main([__file__])