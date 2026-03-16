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
from pathlib import Path
from uuid import uuid4

from langchain_huggingface import HuggingFaceEndpointEmbeddings

from app.conf.config_loader import load_config
from app.conf.meta_config import MetaConfig
from app.models.mysql.column_info_mysql import ColumnInfoMySQL
from app.models.mysql.table_info_mysql import TableInfoMySQL
from app.repository.mysql.dw_mysql_repository import DwMysqlRepository
from app.repository.mysql.meta_mysql_repository import MetaMysqlRepository
from app.models.qdrant.column_info_qdrant import ColumnInfoQdrant
from app.core.logger import logger
from app.models.es.value_info_es import ValueInfoEs
from app.repository.qdrant.column_qdrant_repository import ColumnQdrantRepository
from app.repository.es.value_es_repository import ValueEsRepository
from app.models.mysql.column_metric_mysql import ColumnMetricMySQL
from app.models.mysql.metric_info_mysql import MetricInfoMySQL
from app.models.qdrant.metric_info_qdrant import MetricInfoQdrant
from app.repository.qdrant.metric_qdrant_repository import MetricQdrantRepository


class MetaKnowledgeService:
    def __init__(
        self,
        meta_mysql_repository: MetaMysqlRepository,
        dw_mysql_repository: DwMysqlRepository,
        column_qdrant_repository: ColumnQdrantRepository,
        embedding_client: HuggingFaceEndpointEmbeddings,
        value_es_repository: ValueEsRepository,
        metric_qdrant_repository: MetricQdrantRepository,
    ):
        self.meta_mysql_repository = meta_mysql_repository
        self.dw_mysql_repository = dw_mysql_repository
        self.column_qdrant_repository = column_qdrant_repository
        self.embedding_client = embedding_client
        self.value_es_repository = value_es_repository
        self.metric_qdrant_repository = metric_qdrant_repository

    def _convert_column_info_from_mysql_to_qdrant(self, column_info: ColumnInfoMySQL) -> ColumnInfoQdrant:
        return ColumnInfoQdrant(
            id=column_info.id,
            name=column_info.name,
            type=column_info.type,
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

        # 库表中已有数据 -> 此处注释, 测试完成后取消注释
        # async with self.meta_mysql_repository.session.begin():  # 自动维护事务 -> 成功：自动提交事务；失败：自动回滚事务
        #     await self.meta_mysql_repository.save_tables_info(tables_info)
        #     await self.meta_mysql_repository.save_columns_info(columns_info)

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

    async def _save_column_values_info_to_es(self, meta_config: MetaConfig, columns_info: list[ColumnInfoMySQL]):
        """"""

        """ 1. 确保 index 存在 """
        await self.value_es_repository.ensure_create_index()

        """ 2. 将需要存储的字段值同步至es """
        # 在 conf/meta_config.yaml 中 columns.sync 配置为 true 的字段才会存储至es

        col2sync: dict[str, bool] = {} # 每个字段是否需要进行同步
        for table in meta_config.tables:
            for column in table.columns:
                col2sync[f'{table.name}.{column.name}'] = column.sync

        value_infos: list[ValueInfoEs] = [] # 定义全局的 ValueInfoEs 实体类列表, 用于后面分批存储至es

        for col_info in columns_info:
            sync = col2sync[col_info.id]
            if sync:
                """ 2.1 查询当前字段的所有值 """
                table_name = col_info.table_id
                column_name = col_info.name
                col_values: list = await self.dw_mysql_repository.get_column_values(
                    table_name=table_name,
                    column_name=column_name,
                    limit=100000 # 维度值不会太多, 这里给一个很高的上限来确保所有值都能被取到
                )

                """ 2.2 封装为 ValueInfoEs 实体类 """
                curr_val_infos = [ ValueInfoEs(
                    id=f'{col_info.id}.{value}',
                    value=value,
                    type=col_info.type,
                    column_id=col_info.id,
                    column_name=col_info.name,
                    table_id=col_info.table_id,
                    table_name=col_info.table_id
                ) for value in col_values ]

                value_infos.extend(curr_val_infos)
        """ 2.3 分批写入es """
        await self.value_es_repository.index(value_infos=value_infos)

    async def _save_metrics_to_meta_db(self, meta_config):
        """ 3.1.1 将指标的配置信息封装为MetricInfoMySQL和ColumnMetricMySQL实体类对象 """
        metric_infos: list[MetricInfoMySQL] = []
        column_metric_infos: list[ColumnMetricMySQL] = []

        for metric in meta_config.metrics:
            metric_infos.append(MetricInfoMySQL(
                id=metric.name,
                name=metric.name,
                description=metric.description,
                relevant_columns=metric.relevant_columns,
                alias=metric.alias
            ))
            for column in metric.relevant_columns:
                column_metric_infos.append(ColumnMetricMySQL(
                    column_id=column,
                    metric_id=metric.name
                ))

        """ 3.1.2 通过repository层将封装的指标信息进行持久化保存: 涉及写操作，注意事务 """
        async with self.meta_mysql_repository.session.begin():
            await self.meta_mysql_repository.save_metric_infos(metric_infos=metric_infos)
            await self.meta_mysql_repository.save_column_metric_infos(column_metric_infos=column_metric_infos)

        return metric_infos

    def _convert_metric_info_from_mysql_to_qdrant(self, metric_info: MetricInfoMySQL) -> MetricInfoQdrant:
        return MetricInfoQdrant(
            id=metric_info.id,
            name=metric_info.name,
            description=metric_info.description,
            relevant_columns=metric_info.relevant_columns,
            alias=metric_info.alias
        )

    async def _save_metric_info_to_qdrant(self, metric_infos: list[MetricInfoMySQL]):
        """ 3.2.1 将指标信息填充至 points 列表中 """
        points: list[dict] = [] # 收集了所有要往qdrant中存放的数据

        for metric_info in metric_infos:
            points.append({
                "id": uuid4(), # 一个指标会有多个点, 这里不能使用指标的id, 多point会冲突
                "embedding_text": metric_info.name,
                "payload": self._convert_metric_info_from_mysql_to_qdrant(metric_info=metric_info) # 整个指标信息放进来，省得后续再进行查库操作
            })
            points.append({
                "id": uuid4(),
                "embedding_text": metric_info.description,
                "payload": self._convert_metric_info_from_mysql_to_qdrant(metric_info=metric_info)
            })
            for alia in metric_info.alias:
                points.append({
                    "id": uuid4(),
                    "embedding_text": alia,
                    "payload": self._convert_metric_info_from_mysql_to_qdrant(metric_info=metric_info)
                })

        """ 3.2.2 将搜集的 points 存储至 qdrant 中 """
        ids = [ point['id'] for point in points ]

        vectors: list[list[float]] = []
        embedding_texts = [point['embedding_text'] for point in points]
        embedding_batch_size = 10
        for i in range(0, len(embedding_texts), embedding_batch_size):
            embeddings = await self.embedding_client.aembed_documents(
                texts=embedding_texts[i:i+embedding_batch_size]
            )
            vectors.extend(embeddings)

        pay_loads = [ point['payload'] for point in points ]

        """ ① 确保collection已被事先创建 """
        await self.metric_qdrant_repository.ensure_create_collection()

        """ ② 将数据批量插入至qdrant中 """
        await self.metric_qdrant_repository.upsert_points(
            ids=ids,
            vectors=vectors,
            payloads=pay_loads
        )

    async def build(self, config_path: Path):
        # TODO 1. 加载 conf/meta_config.yaml 文件配置
        meta_config: MetaConfig = load_config(
            config_path = config_path,
            Schema_cls = MetaConfig
        )
        logger.info("加载配置文件")
        # TODO 2. 处理表信息
        if meta_config.tables:
            # 2.1 保存表信息至meta数据库
            tables_info, columns_info = await self._save_tables_to_meta_db(meta_config=meta_config)
            logger.info("保存表信息到meta数据库")

            # 2.2 为字段信息创建向量索引
            await self._save_column_info_to_qdrant(columns_info=columns_info)
            logger.info("为字段信息创建向量索引")

            # 2.3 为字段值信息创建全文索引
            await self._save_column_values_info_to_es(meta_config=meta_config, columns_info=columns_info)
            logger.info("为字段值信息创建全文索引")

        # TODO 3. 处理指标信息
        if meta_config.metrics:
            # 3.1 保存指标信息至meta数据库
            metric_infos = await self._save_metrics_to_meta_db(meta_config=meta_config)
            logger.info("保存指标信息到meta数据库")

            # 3.2 为指标信息创建向量索引
            await self._save_metric_info_to_qdrant(metric_infos=metric_infos)
            logger.info("为指标信息创建向量索引")

        logger.info("元数据知识库构建完成！")





