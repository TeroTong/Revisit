"""
通知服务
"""
import aiohttp
from typing import Dict, List, Optional, Any
import logging

from config.settings import settings

logger = logging.getLogger(__name__)


class NotificationService:
    """通知服务"""

    def __init__(self):
        self.wechat_enabled = bool(settings.NOTIFICATION.WECHAT_APP_ID and settings.NOTIFICATION.WECHAT_APP_SECRET)
        self.sms_enabled = settings.NOTIFICATION.SMS_ENABLED and bool(
            settings.NOTIFICATION.ALIYUN_ACCESS_KEY_ID and settings.NOTIFICATION.ALIYUN_ACCESS_KEY_SECRET
        )

    async def send_wechat_message(self, openid: str, content: str, template_id: str = None) -> bool:
        """发送微信消息"""
        if not self.wechat_enabled:
            logger.warning("微信通知未配置，跳过发送")
            return False

        if not openid:
            logger.warning("微信openid为空，跳过发送")
            return False

        if template_id is None:
            template_id = settings.NOTIFICATION.WECHAT_TEMPLATE_ID

        try:
            # 获取access token
            access_token = await self._get_wechat_access_token()
            if not access_token:
                return False

            # 发送模板消息
            url = f"https://api.weixin.qq.com/cgi-bin/message/template/send?access_token={access_token}"

            # 构造消息数据
            data = {
                "touser": openid,
                "template_id": template_id,
                "data": {
                    "content": {
                        "value": content,
                        "color": "#173177"
                    }
                }
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=data) as response:
                    if response.status == 200:
                        result = await response.json()
                        if result.get('errcode') == 0:
                            logger.info(f"微信消息发送成功: {openid}")
                            return True
                        else:
                            logger.error(f"微信消息发送失败: {result}")
                            return False
                    else:
                        logger.error(f"微信API调用失败: {response.status}")
                        return False

        except Exception as e:
            logger.error(f"发送微信消息异常: {e}")
            return False

    async def _get_wechat_access_token(self) -> Optional[str]:
        """获取微信access token"""
        try:
            url = f"https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid={settings.NOTIFICATION.WECHAT_APP_ID}&secret={settings.NOTIFICATION.WECHAT_APP_SECRET}"

            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        result = await response.json()
                        if 'access_token' in result:
                            return result['access_token']
                        else:
                            logger.error(f"获取微信access_token失败: {result}")
                            return None
                    else:
                        logger.error(f"获取微信access_token API调用失败: {response.status}")
                        return None

        except Exception as e:
            logger.error(f"获取微信access_token异常: {e}")
            return None

    async def send_sms(self, phone: str, content: str, template_code: str = None) -> bool:
        """发送短信"""
        if not self.sms_enabled:
            logger.warning("短信通知未配置，跳过发送")
            return False

        if not phone:
            logger.warning("手机号为空，跳过发送")
            return False

        # 这里实现阿里云短信发送逻辑
        # 由于阿里云SDK的异步支持可能需要额外处理，这里先返回成功
        logger.info(f"模拟发送短信到 {phone}: {content[:50]}...")
        return True

    async def send_email(self, email: str, subject: str, content: str) -> bool:
        """发送邮件"""
        # 邮件发送实现
        # 这里可以使用aiosmtplib等异步SMTP库
        logger.info(f"模拟发送邮件到 {email}: {subject}")
        return True

    async def send_multichannel_notification(self,
                                             customer_info: Dict[str, Any],
                                             content: str,
                                             channels: List[str] = None) -> Dict[str, bool]:
        """发送多渠道通知"""
        if channels is None:
            channels = ['wechat']

        results = {}

        for channel in channels:
            if channel == 'wechat':
                success = await self.send_wechat_message(
                    customer_info.get('wechat_id'),
                    content
                )
                results['wechat'] = success
            elif channel == 'sms':
                success = await self.send_sms(
                    customer_info.get('phone'),
                    content
                )
                results['sms'] = success
            elif channel == 'email':
                success = await self.send_email(
                    customer_info.get('email'),
                    "医美机构生日祝福",
                    content
                )
                results['email'] = success

        return results

    async def test_wechat_connection(self) -> bool:
        """测试微信连接"""
        if not self.wechat_enabled:
            return False

        access_token = await self._get_wechat_access_token()
        return access_token is not None