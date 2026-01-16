"""
LLM服务

支持多种 LLM 提供商（OpenAI/DeepSeek 等）
"""
import aiohttp
import asyncio
import json
from typing import Dict, List, Any, Optional
import logging

from config.settings import settings

logger = logging.getLogger(__name__)


class LLMService:
    """LLM服务"""

    def __init__(self, api_key: str = None, api_url: str = None, model: str = None):
        self.api_key = api_key or settings.LLM.OPENAI_API_KEY
        self.api_url = api_url or settings.LLM.OPENAI_API_BASE
        self.model = model or settings.LLM.OPENAI_MODEL
        self.max_tokens = settings.LLM.OPENAI_MAX_TOKENS
        self.temperature = settings.LLM.OPENAI_TEMPERATURE

        if not self.api_key or self.api_key.startswith('sk-xxx'):
            logger.warning("LLM API Key未正确设置，LLM功能将使用默认响应")
            self._enabled = False
        else:
            self._enabled = True

    @property
    def enabled(self) -> bool:
        return self._enabled

    async def generate_content(self, prompt: str, system_prompt: str = None) -> str:
        """生成内容"""
        if not self._enabled:
            logger.debug("LLM未启用，返回空内容")
            return ""

        if system_prompt is None:
            system_prompt = "你是一位专业的医美机构客户关系专员。请根据提供的信息，生成专业、亲切的回复。"

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ]

        try:
            response = await self._call_api(messages)

            if response and 'choices' in response and len(response['choices']) > 0:
                content = response['choices'][0]['message']['content']
                return content.strip()
            else:
                logger.error(f"LLM API返回异常: {response}")
                return ""

        except Exception as e:
            logger.error(f"调用LLM API失败: {e}")
            return ""

    async def generate_birthday_message(
        self,
        customer_info: Dict[str, Any],
        history: Dict[str, Any]
    ) -> Dict[str, Any]:
        """生成生日消息（结构化输出）"""
        prompt = f"""
客户信息：
{json.dumps(customer_info, ensure_ascii=False, indent=2)}

历史记录：
{json.dumps(history, ensure_ascii=False, indent=2)}

请生成个性化的生日祝福，以JSON格式返回，包含以下字段：
- message: 生日祝福消息（200字以内）
- recommendations: 推荐列表，每个推荐包含 type（project/product）、name、reason
- offer: 专属优惠信息

只返回JSON，不要其他内容。
"""

        system_prompt = "你是一位专业的医美机构客户关系专员。请根据客户信息和历史记录，生成个性化的生日祝福。请以纯JSON格式返回。"

        content = await self.generate_content(prompt, system_prompt)

        try:
            # 尝试解析JSON
            # 清理可能的 markdown 代码块标记
            if content.startswith('```'):
                content = content.split('\n', 1)[1]
            if content.endswith('```'):
                content = content.rsplit('\n', 1)[0]
            content = content.strip()

            result = json.loads(content)
            return result
        except json.JSONDecodeError:
            logger.warning("LLM返回的不是JSON格式，使用原始内容")
            return {
                "message": content if content else "祝您生日快乐！",
                "recommendations": [],
                "offer": "生日月内消费享8.8折优惠"
            }

    async def generate_project_recommendation(
        self,
        customer_info: Dict[str, Any],
        consumption_history: List[Dict]
    ) -> List[Dict]:
        """生成项目推荐"""
        if not self._enabled:
            return []

        prompt = f"""
基于以下客户信息和消费历史，推荐3个适合的医美项目：

客户信息：
- 性别：{customer_info.get('gender', '未知')}
- 年龄：{customer_info.get('age', '未知')}
- 会员等级：{customer_info.get('vip_level', '普通')}
- 累计消费：{customer_info.get('total_consumption', 0)}元

历史消费：
{json.dumps(consumption_history[:5], ensure_ascii=False, indent=2) if consumption_history else '无'}

请以JSON数组格式返回推荐项目，每个项目包含：
- category: 项目类别
- name: 项目名称
- reason: 推荐理由
- suitable_for: 适合人群
"""

        content = await self.generate_content(prompt)

        try:
            if content.startswith('```'):
                content = content.split('\n', 1)[1]
            if content.endswith('```'):
                content = content.rsplit('\n', 1)[0]
            return json.loads(content.strip())
        except:
            return []

    async def _call_api(self, messages: List[Dict]) -> Dict:
        """调用 LLM API"""
        # 构建请求头
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        # 构建请求体
        data = {
            "model": self.model,
            "messages": messages,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens
        }

        # 判断 API URL 格式（兼容不同提供商）
        api_endpoint = self.api_url
        if not api_endpoint.endswith('/chat/completions'):
            if api_endpoint.endswith('/'):
                api_endpoint = api_endpoint.rstrip('/')
            if not api_endpoint.endswith('/v1'):
                api_endpoint = f"{api_endpoint}/v1"
            api_endpoint = f"{api_endpoint}/chat/completions"

        timeout = aiohttp.ClientTimeout(total=60)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.post(
                    api_endpoint,
                    headers=headers,
                    json=data
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result
                    else:
                        error_text = await response.text()
                        logger.error(f"LLM API调用失败: {response.status}, {error_text}")
                        raise Exception(f"API调用失败: {response.status}")

            except aiohttp.ClientError as e:
                logger.error(f"HTTP客户端错误: {e}")
                raise
            except asyncio.TimeoutError:
                logger.error("LLM API调用超时")
                raise

    async def test_connection(self) -> bool:
        """测试连接"""
        if not self._enabled:
            return False

        try:
            test_prompt = "请回复数字123"
            response = await self.generate_content(test_prompt, "你是一个测试助手。只需回复123。")

            if response and '123' in response:
                logger.info("✅ LLM连接测试成功")
                return True
            else:
                logger.warning(f"LLM连接测试返回异常: {response}")
                return False
        except Exception as e:
            logger.error(f"LLM连接测试失败: {e}")
            return False


# 便捷函数
async def generate_birthday_content(customer: Dict, history: List[Dict]) -> str:
    """便捷函数：生成生日回访内容"""
    service = LLMService()
    result = await service.generate_birthday_message(customer, {"consumptions": history})
    return result.get("message", "祝您生日快乐！")
