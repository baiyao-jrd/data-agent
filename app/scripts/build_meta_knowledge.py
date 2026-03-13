"""
------------------------------------------------
    @Time: 2026/3/13 11:50
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/build_meta_knowledge.py
    @Software: PyCharm
    @Description: 执行脚本 同步数据
        加载 conf/meta_config.yaml 文件配置
        读数据库 -> 整合 -> 存储
------------------------------------------------
"""
import argparse
import asyncio
from pathlib import Path

from app.core.logger import logger
from app.core.context import request_id_ctx_var
from app.service.meta_knowledge_service import MetaKnowledgeService
from clients.mysql_client_manager import meta_mysql_client_manager
from repository.meta_mysql_repository import MetaMysqlRepository


async def build(config_path: Path):
    """  共三层：client -> repository -> service """
    meta_mysql_client_manager.init()
    async with meta_mysql_client_manager.session_factory() as session:
        meta_mysql_repository = MetaMysqlRepository(session)
        meta_knowledge_service = MetaKnowledgeService(meta_mysql_repository)
        await meta_knowledge_service.build(config_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--conf')
    args = parser.parse_args()

    config_path = Path(args.conf)

    asyncio.run(build(config_path))