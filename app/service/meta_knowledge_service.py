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
from uuid import uuid4

from langchain_huggingface import HuggingFaceEndpointEmbeddings
from omegaconf import OmegaConf

from app.conf.config_loader import load_config
from app.conf.meta_config import MetaConfig
from app.models.mysql.column_info_mysql import ColumnInfoMySQL
from app.models.mysql.table_info_mysql import TableInfoMySQL
from app.repository.dw_mysql_repository import DwMysqlRepository
from app.repository.meta_mysql_repository import MetaMysqlRepository
from models.qdrant.column_info_qdrant import ColumnInfoQdrant
from repository.column_qdrant_repository import ColumnQdrantRepository


class MetaKnowledgeService:
    def __init__(
        self,
        meta_mysql_repository: MetaMysqlRepository,
        dw_mysql_repository: DwMysqlRepository,
        column_qdrant_repository: ColumnQdrantRepository,
        embedding_client: HuggingFaceEndpointEmbeddings
    ):
        self.meta_mysql_repository = meta_mysql_repository
        self.dw_mysql_repository = dw_mysql_repository
        self.column_qdrant_repository = column_qdrant_repository
        self.embedding_client = embedding_client

    def _convert_column_info_from_mysql_to_qdrant(self, column_info: ColumnInfoMySQL) -> ColumnInfoQdrant:
        return ColumnInfoQdrant(
            id=column_info.id,
            name=column_info.name,
            role=column_info.role,
            examples=column_info.examples,
            description=column_info.description,
            alias=column_info.alias,
            table_id=column_info.table_id
        )

    async def _save_tables_to_meta_db(self, meta_config: MetaConfig):
        """ 为批量保存，事先设置两个存储实体类对象的列表 """
        tables_info: list[TableInfoMySQL] = []
        columns_info: list[ColumnInfoMySQL] = []
        for table in meta_config.tables:  # table -> TableInfoMySQL
            tables_info.append(
                TableInfoMySQL(
                    id=table.name,  # 同一个库表明一定唯一
                    name=table.name,
                    role=table.role,
                    description=table.description
                )
            )
            """ 查询该表的所有字段类型 """
            column_types: dict[str, str] = await self.dw_mysql_repository.get_column_types(table_name=table.name)
            for column in table.columns:  # column -> ColumnInfoMySQL
                """ 查询字段所有值 """
                column_values: list = await self.dw_mysql_repository.get_column_values(
                    table_name=table.name,
                    column_name=column.name,
                    limit=10
                )
                columns_info.append(
                    ColumnInfoMySQL(
                        id=f"{table.name}.{column.name}",
                        name=column.name,
                        type=column_types[column.name],  # 需要查库表 -> dw
                        role=column.role,
                        examples=column_values,  # 需要查库表 -> dw
                        description=column.description,
                        alias=column.alias,
                        table_id=table.name
                    )
                )

        async with self.meta_mysql_repository.session.begin():  # 自动维护事务 -> 成功：自动提交事务；失败：自动回滚事务
            await self.meta_mysql_repository.save_tables_info(tables_info)
            await self.meta_mysql_repository.save_columns_info(columns_info)

        return tables_info, columns_info

    async def _save_column_info_to_qdrant(self, columns_info: list[ColumnInfoMySQL]):
        """ 1. 确保创建 collection """
        await self.column_qdrant_repository.ensure_create_collection()

        """ 2. 将 name, description, alias 存放至points中 """
        points: list[dict] = []
        for col_info in columns_info:
            points.append({
                "id": uuid4(),
                "embedding_text": col_info.name,
                "payload": self._convert_column_info_from_mysql_to_qdrant(column_info=col_info)
                # 这里可以只放 column_info 的id，这样检索到某字段之后，就可以通过id查询到该字段的所有信息（咱们这里选择都放进去）
            })

            points.append({
                "id": uuid4(),
                "embedding_text": col_info.description,
                "payload": self._convert_column_info_from_mysql_to_qdrant(column_info=col_info)
            })

            for alia in col_info.alias:
                points.append({
                    "id": uuid4(),
                    "embedding_text": alia,
                    "payload": self._convert_column_info_from_mysql_to_qdrant(column_info=col_info)
                })
        """ 3. 取出所有的 embedding_text """
        embedding_texts: list[str] = [point["embedding_text"] for point in points]

        """" 4. 按批进行向量化 """
        embedding_vectors: list[list[float]] = []
        embedding_batch_size = 10
        for i in range(0, len(embedding_texts), embedding_batch_size):  # 步长为批次大小
            embedding_texts_batch = embedding_texts[i:i + embedding_batch_size]
            vectors = await self.embedding_client.aembed_documents(texts=embedding_texts_batch)
            embedding_vectors.extend(vectors)

        """ 5. 组装PointStruct -> 在repository层做 """
        ids = [point["id"] for point in points]
        payloads = [point["payload"] for point in points]
        await self.column_qdrant_repository.upsert_points(
            ids=ids,
            embeddings=embedding_vectors,
            payloads=payloads
        )

    async def build(self, config_path: Path):
        # TODO 1. 加载 conf/meta_config.yaml 文件配置
        meta_config: MetaConfig = load_config(
            config_path = config_path,
            Schema_cls = MetaConfig
        )
        # TODO 2. 处理表信息
        if meta_config.tables:
            # 2.1 保存表信息至meta数据库
            tables_info, columns_info = await self._save_tables_to_meta_db(meta_config=meta_config)

            # 2.2 为字段信息创建向量索引
            await self._save_column_info_to_qdrant(columns_info=columns_info)


            # 2.3 为字段值信息创建全文索引
            pass

        # TODO 3. 处理指标信息
        if meta_config.metrics:
            # 3.1 保存指标信息至meta数据库
            # 3.2 为指标信息创建向量索引
            # 3.3 为指标值信息创建全文索引
            pass