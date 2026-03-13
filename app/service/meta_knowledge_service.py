"""
------------------------------------------------
    @Time: 2026/3/13 13:57 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/meta_knowledge_service.py
    @Software: PyCharm
    @Description:
------------------------------------------------
"""
import asyncio
from pathlib import Path

from omegaconf import OmegaConf

from app.conf.config_loader import load_config
from app.conf.meta_config import MetaConfig
from models.mysql.column_info_mysql import ColumnInfoMySQL
from models.mysql.table_info_mysql import TableInfoMySQL
from repository.meta_mysql_repository import MetaMysqlRepository


class MetaKnowledgeService:
    def __init__(self, meta_mysql_repository: MetaMysqlRepository):
        self.meta_mysql_repository = meta_mysql_repository
        pass

    async def build(self, config_path: Path):
        # TODO 1. 加载 conf/meta_config.yaml 文件配置
        meta_config: MetaConfig = load_config(
            config_path = config_path,
            Schema_cls = MetaConfig
        )
        # TODO 2. 处理表信息
        if meta_config.tables:
            # 2.1 保存表信息至meta数据库
            """ 为批量保存，事先设置两个存储实体类对象的列表 """
            tables_info: list[TableInfoMySQL] = []
            columns_info: list[ColumnInfoMySQL] = []
            for table in meta_config.tables: # table -> TableInfoMySQL
                tables_info.append(
                    TableInfoMySQL(
                        id=table.name, # 同一个库表明一定唯一
                        name=table.name,
                        role=table.role,
                        description=table.description
                    )
                )
                for column in table.columns: # column -> ColumnInfoMySQL
                    columns_info.append(
                        ColumnInfoMySQL(
                            id=f"{table.name}.{column.name}",
                            name=column.name,
                            type=None, # 需要查库表 -> dw
                            role=column.role,
                            examples=None, # 需要查库表 -> dw
                            description=column.description,
                            alias=column.alias,
                            table_id=table.name
                        )
                    )
            await self.meta_mysql_repository.save_tables_info(tables_info)
            await self.meta_mysql_repository.save_columns_info(columns_info)

            # 2.2 为字段信息创建向量索引
            # 2.3 为字段值信息创建全文索引
            pass

        # TODO 3. 处理指标信息
        if meta_config.metrics:
            # 3.1 保存指标信息至meta数据库
            # 3.2 为指标信息创建向量索引
            # 3.3 为指标值信息创建全文索引
            pass