"""
------------------------------------------------
    @Time: 2026/3/16 16:09 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/context.py
    @Software: PyCharm
    @Description:
------------------------------------------------
"""
from typing import TypedDict

from langchain_huggingface import HuggingFaceEndpointEmbeddings

from repository.es.value_es_repository import ValueEsRepository
from repository.mysql.meta_mysql_repository import MetaMysqlRepository
from repository.qdrant.column_qdrant_repository import ColumnQdrantRepository
from repository.qdrant.metric_qdrant_repository import MetricQdrantRepository

class DataAgentContext(TypedDict):
    embedding_client: HuggingFaceEndpointEmbeddings
    column_qdrant_repository: ColumnQdrantRepository
    value_es_repository: ValueEsRepository
    metric_qdrant_repository: MetricQdrantRepository
    meta_mysql_repository: MetaMysqlRepository