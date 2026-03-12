"""
------------------------------------------------
    @Time: 2026/3/12 16:57 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/logger.py
    @Software: PyCharm
    @Description: 日志管理
------------------------------------------------
"""
import asyncio
import sys
import uuid
from pathlib import Path

from loguru import logger

from app.conf.app_config import app_config
from app.core.context import request_id_ctx_var

log_format = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<magenta>request_id - {extra[request_id]}</magenta> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>"
)


def inject_request_id(record):
    """
    向一条日志中注入 request_id
    :param record:
    :return:
    """
    try:
        request_id = request_id_ctx_var.get()
    except Exception as e:
        request_id = uuid.uuid4()
    record["extra"]["request_id"] = request_id # 向 record 的 extra 属性中添加 request_id 字段


logger.remove() # 清除默认配置
logger = logger.patch(inject_request_id) # 添加补丁
if app_config.logging.console.enable: # 配置文件允许控制台输出日志
    logger.add(
        sink=sys.stdout, # logger输出目的地：控制台
        level=app_config.logging.console.level,
        format=log_format # 日志格式模版
    )
if app_config.logging.file.enable: # 配置文件允许文件输出日志
    path = Path(app_config.logging.file.path)
    path.mkdir(parents=True, exist_ok=True)
    logger.add(
        sink=path / "app.log", # logger输出目的地：文件
        level=app_config.logging.file.level,
        format=log_format,
        rotation=app_config.logging.file.rotation,
        retention=app_config.logging.file.retention,
        encoding="utf-8"
    )

async def task_one():
    request_id_ctx_var.set("Graph 1")
    await asyncio.sleep(2)
    logger.info(f"task_one: {await graph()}")

async def task_two():
    request_id_ctx_var.set("Graph 2")
    await asyncio.sleep(2)
    logger.info(f"task_two: {await graph()}")

async def graph():
    return request_id_ctx_var.get()

async def main():
    await asyncio.gather(task_one(), task_two())

if __name__ == '__main__':
    asyncio.run(main())
# if __name__ == '__main__':
#     logger.info("hello world")