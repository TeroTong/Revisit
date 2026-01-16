"""
日志配置
"""
import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler
from config.settings import settings


def setup_logger(name: str = None, log_file: str = None, level: str = None):
    """设置日志记录器"""

    if level is None:
        level = settings.APP.LOG_LEVEL

    if log_file is None:
        log_file = Path(settings.APP.LOG_DIR) / "app.log"
    else:
        log_file = Path(settings.APP.LOG_DIR) / log_file

    # 确保日志目录存在
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # 创建记录器
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))

    # 清除已有的处理器
    logger.handlers.clear()

    # 创建格式化器
    formatter = logging.Formatter(
        fmt=settings.APP.LOG_FORMAT,
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, level.upper()))
    console_handler.setFormatter(formatter)

    # 文件处理器（按大小轮转）
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(getattr(logging, level.upper()))
    file_handler.setFormatter(formatter)

    # 添加处理器
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


# 创建默认的日志记录器
logger = setup_logger("medical_beauty_reminder")


def get_logger(name: str = None):
    """获取日志记录器"""
    if name:
        return logging.getLogger(name)
    else:
        return logger


def safe_print(msg: str):
    """安全打印，处理Windows GBK编码问题"""
    try:
        print(msg)
    except UnicodeEncodeError:
        # 替换emoji为ASCII字符
        safe_msg = msg.replace('✅', '[OK]').replace('❌', '[FAIL]').replace('⚠️', '[WARN]')
        print(safe_msg)
