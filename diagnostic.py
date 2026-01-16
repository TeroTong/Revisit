#!/usr/bin/env python3
"""
Qdrant服务诊断脚本
"""
import os

# 禁用代理设置（解决远程数据库连接问题）
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    os.environ.pop(key, None)

import sys
from pathlib import Path

# 添加项目根目录到Python路径（此文件在项目根目录）
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root.absolute()))

import logging
import time
import socket
import requests
from qdrant_client import QdrantClient
from database.qdrant.connection import _get_qdrant_host_port

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_port(host: str, port: int, timeout: int = 5) -> bool:
    """检查端口是否开放"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception as e:
        logger.debug(f"检查端口失败: {e}")
        return False


def check_http(host: str, port: int, timeout: int = 5) -> bool:
    """检查HTTP服务是否响应"""
    try:
        response = requests.get(f'http://{host}:{port}', timeout=timeout)
        return response.status_code == 200
    except Exception as e:
        logger.debug(f"检查HTTP失败: {e}")
        return False


def check_qdrant_api(host: str, port: int, timeout: int = 5) -> bool:
    """检查Qdrant API是否可用"""
    try:
        client = QdrantClient(host=host, port=port, timeout=timeout)
        collections = client.get_collections()
        return True
    except Exception as e:
        logger.debug(f"检查Qdrant API失败: {e}")
        return False


def check_docker_status() -> bool:
    """检查Docker容器状态"""
    import subprocess
    try:
        result = subprocess.run(
            ['docker', 'ps', '--filter', 'name=qdrant_server', '--format', '{{.Status}}'],
            capture_output=True,
            text=True
        )
        if result.returncode == 0 and result.stdout.strip():
            status = result.stdout.strip()
            logger.info(f"容器状态: {status}")

            # 获取更详细的信息
            result = subprocess.run(
                ['docker', 'inspect', 'qdrant_server', '--format', '{{.State.Status}}'],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                state = result.stdout.strip()
                logger.info(f"容器详细状态: {state}")
                return state == 'running'
        return False
    except Exception as e:
        logger.error(f"检查Docker状态失败: {e}")
        return False


def get_qdrant_logs(tail: int = 10) -> str:
    """获取Qdrant日志"""
    import subprocess
    try:
        result = subprocess.run(
            ['docker', 'logs', '--tail', str(tail), 'qdrant_server'],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            return result.stdout
        else:
            return f"获取日志失败: {result.stderr}"
    except Exception as e:
        return f"执行命令失败: {e}"


def main():
    """主函数"""
    print("=" * 60)
    print("Qdrant服务状态检查")
    print("=" * 60)

    host, port = _get_qdrant_host_port()

    print(f"\n检查目标: {host}:{port}")

    # 检查Docker容器
    print("\n1. 检查Docker容器状态...")
    if check_docker_status():
        print("✅ Qdrant容器正在运行")
    else:
        print("❌ Qdrant容器未运行或状态异常")
        print("\n尝试启动容器...")
        try:
            import subprocess
            result = subprocess.run(
                ['docker', 'start', 'qdrant_server'],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                print("✅ 容器启动成功，等待10秒...")
                time.sleep(10)
            else:
                print(f"❌ 容器启动失败: {result.stderr}")
        except Exception as e:
            print(f"❌ 启动容器失败: {e}")

    # 检查端口
    print("\n2. 检查端口状态...")
    if check_port(host, port):
        print(f"✅ 端口 {port} 已开放")
    else:
        print(f"❌ 端口 {port} 未开放")

    # 检查HTTP服务
    print("\n3. 检查HTTP服务...")
    if check_http(host, port):
        print("✅ HTTP服务正常")
    else:
        print("❌ HTTP服务无响应")

    # 检查Qdrant API
    print("\n4. 检查Qdrant API...")
    if check_qdrant_api(host, port):
        print("✅ Qdrant API正常")
    else:
        print("❌ Qdrant API无响应")

    # 显示日志
    print("\n5. 查看Qdrant日志（最后10行）...")
    logs = get_qdrant_logs()
    print("日志内容:")
    print("-" * 40)
    print(logs)
    print("-" * 40)

    # 汇总结果
    print("\n" + "=" * 60)
    print("检查结果汇总:")

    if check_port(host, port) and check_http(host, port) and check_qdrant_api(host, port):
        print("✅ Qdrant服务完全正常，可以初始化")
    elif check_port(host, port) and check_http(host, port):
        print("⚠️ Qdrant服务部分正常（端口和HTTP可用，但API可能还在启动中）")
        print("   建议等待30秒后重试")
    elif check_port(host, port):
        print("⚠️ 端口已开放但服务未完全启动")
        print("   建议等待并查看日志")
    else:
        print("❌ Qdrant服务不可用")
        print("   请检查容器状态和日志")

    print("=" * 60)


if __name__ == "__main__":
    main()