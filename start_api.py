"""
启动 API 服务器

用法：
    python start_api.py                 # 默认绑定到 0.0.0.0:8000
    python start_api.py --port 8080     # 自定义端口
    python start_api.py --host 127.0.0.1  # 只允许本地访问

医美机构可以通过以下方式从其他电脑访问：
    http://<服务器IP>:8000/docs          # API文档
    http://<服务器IP>:8000/api/v1/reminders/<机构代码>/upcoming-birthdays  # 即将生日客户
"""
import os

# 禁用代理设置（解决远程数据库连接问题）
for key in ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']:
    os.environ.pop(key, None)

import argparse
import uvicorn
import socket


def get_local_ip():
    """获取本机IP地址"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def main():
    parser = argparse.ArgumentParser(
        description="医美客户回访系统 - API 服务器",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python start_api.py                    # 启动服务（允许远程访问）
  python start_api.py --port 8080        # 使用自定义端口
  python start_api.py --host 127.0.0.1   # 只允许本地访问
  python start_api.py --reload           # 开发模式（自动重载）
        """
    )
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="绑定的主机地址 (默认: 0.0.0.0, 允许所有IP访问)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="服务端口 (默认: 8000)"
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="开启自动重载（开发模式）"
    )

    args = parser.parse_args()

    local_ip = get_local_ip()

    print("=" * 60)
    print("  医美客户回访系统 - API 服务器")
    print("=" * 60)
    print(f"\n本机IP地址: {local_ip}")
    print(f"服务地址: http://{args.host}:{args.port}")
    print(f"\n其他电脑可以通过以下地址访问：")
    print(f"  📖 API 文档:  http://{local_ip}:{args.port}/docs")
    print(f"  🏥 健康检查:  http://{local_ip}:{args.port}/health")
    print(f"\n常用 API 接口:")
    print(f"  📅 即将生日客户:  GET  /api/v1/reminders/{{机构代码}}/upcoming-birthdays")
    print(f"  📝 生成回访话术:  POST /api/v1/reminders/{{机构代码}}/generate-content")
    print(f"  📊 客户消费历史:  GET  /api/v1/reminders/{{机构代码}}/customer/{{客户代码}}/history")
    print("=" * 60)
    print("\n按 Ctrl+C 停止服务\n")

    uvicorn.run(
        "api.main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level="info"
    )


if __name__ == "__main__":
    main()

