"""
文件操作工具
"""
import json
import csv
import aiofiles
from pathlib import Path
from typing import Any, Dict, List, Union
import logging

logger = logging.getLogger(__name__)


class FileUtils:
    """文件工具类"""

    @staticmethod
    async def read_json(file_path: Union[str, Path]) -> Any:
        """异步读取JSON文件"""
        try:
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                content = await f.read()
                return json.loads(content)
        except FileNotFoundError:
            logger.error(f"文件不存在: {file_path}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析失败: {file_path}, 错误: {e}")
            return None
        except Exception as e:
            logger.error(f"读取文件失败: {file_path}, 错误: {e}")
            return None

    @staticmethod
    async def write_json(file_path: Union[str, Path], data: Any, indent: int = 2):
        """异步写入JSON文件"""
        try:
            async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
                content = json.dumps(data, ensure_ascii=False, indent=indent)
                await f.write(content)
            return True
        except Exception as e:
            logger.error(f"写入文件失败: {file_path}, 错误: {e}")
            return False

    @staticmethod
    async def read_csv(file_path: Union[str, Path], delimiter: str = ',') -> List[Dict[str, Any]]:
        """异步读取CSV文件"""
        try:
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                content = await f.read()
                lines = content.strip().split('\n')

                if not lines:
                    return []

                # 解析CSV
                reader = csv.DictReader(lines, delimiter=delimiter)
                return list(reader)
        except Exception as e:
            logger.error(f"读取CSV文件失败: {file_path}, 错误: {e}")
            return []

    @staticmethod
    async def write_csv(file_path: Union[str, Path], data: List[Dict[str, Any]], fieldnames: List[str] = None):
        """异步写入CSV文件"""
        try:
            if not data:
                return False

            if fieldnames is None:
                fieldnames = list(data[0].keys())

            async with aiofiles.open(file_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)

                # 写入表头
                header = ','.join(fieldnames) + '\n'
                await f.write(header)

                # 写入数据
                for row in data:
                    line = ','.join(str(row.get(field, '')) for field in fieldnames) + '\n'
                    await f.write(line)

            return True
        except Exception as e:
            logger.error(f"写入CSV文件失败: {file_path}, 错误: {e}")
            return False

    @staticmethod
    def ensure_directory(dir_path: Union[str, Path]) -> bool:
        """确保目录存在"""
        try:
            path = Path(dir_path)
            path.mkdir(parents=True, exist_ok=True)
            return True
        except Exception as e:
            logger.error(f"创建目录失败: {dir_path}, 错误: {e}")
            return False

    @staticmethod
    def get_file_size(file_path: Union[str, Path]) -> int:
        """获取文件大小（字节）"""
        try:
            return Path(file_path).stat().st_size
        except Exception:
            return 0

    @staticmethod
    def file_exists(file_path: Union[str, Path]) -> bool:
        """检查文件是否存在"""
        return Path(file_path).exists()