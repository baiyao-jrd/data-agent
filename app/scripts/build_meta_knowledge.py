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
from app.clients.mysql_client_manager import meta_mysql_client_manager, dw_mysql_client_manager
from app.repository.dw_mysql_repository import DwMysqlRepository
from app.repository.meta_mysql_repository import MetaMysqlRepository
from clients.embedding_client_manager import embedding_client_manager
from clients.qdrant_client_manager import qdrant_client_manager
from repository.column_qdrant_repository import ColumnQdrantRepository


async def build(config_path: Path):
    """  共三层：client -> repository -> service """
    meta_mysql_client_manager.init()
    dw_mysql_client_manager.init()
    qdrant_client_manager.init()
    embedding_client_manager.init()
    async with meta_mysql_client_manager.session_factory() as mysql_session, dw_mysql_client_manager.session_factory() as dw_session:
        meta_mysql_repository = MetaMysqlRepository(mysql_session)
        dw_mysql_repository = DwMysqlRepository(dw_session)
        column_qdrant_repository = ColumnQdrantRepository(qdrant_client_manager.client)
        embedding_client = embedding_client_manager.client

        meta_knowledge_service = MetaKnowledgeService(
            meta_mysql_repository=meta_mysql_repository,
            dw_mysql_repository=dw_mysql_repository,
            column_qdrant_repository=column_qdrant_repository,
            embedding_client=embedding_client
        )
        await meta_knowledge_service.build(config_path)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--conf')
    args = parser.parse_args()

    config_path = Path(args.conf)

    asyncio.run(build(config_path))