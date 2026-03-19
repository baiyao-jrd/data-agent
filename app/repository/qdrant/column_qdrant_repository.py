"""
------------------------------------------------
    @Time: 2026/3/14 14:43 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/column_qdrant_repository.py
    @Software: PyCharm
    @Description:
------------------------------------------------
"""
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance

from app.conf.app_config import app_config
from app.models.qdrant.column_info_qdrant import ColumnInfoQdrant


class ColumnQdrantRepository:
    collection_name: str = "data-agent-column"

    def __init__(self, qdrant_client: AsyncQdrantClient): # 只有持有 qdrant_client 才能进行读写操作
        self.qdrant_client = qdrant_client

    async def ensure_create_collection(self):
        if not await self.qdrant_client.collection_exists(self.collection_name):
            await self.qdrant_client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(
                    size=app_config.qdrant.embedding_size, # 取决于模型 -> bge-large-zh-v1.5 dimension: 1024 ===/ source /=== https://huggingface.co/BAAI/bge-large-zh-v1.5
                    distance=Distance.COSINE
                )
            )

    async def upsert_points(
            self,
            ids: list[str],
            embeddings: list[list[float]],
            payloads: list[ColumnInfoQdrant],
            batch_size: int = 20 # 按批对client发起请求 -> 一次发起请求数不宜太多
    ):
        zipped = list(zip(ids, embeddings, payloads)) # zip之后是一个迭代器, 需转为列表后才能进行切片
        for i in range(0, len(zipped), batch_size):
            batch = zipped[i:i+batch_size] # 每个元素是一个三元组
            points = [ PointStruct(
                id=id,
                vector=embedding,
                payload=payload
            ) for id, embedding, payload in batch ]
            await self.qdrant_client.upsert(
                collection_name=self.collection_name,
                points=points
            )

    async def aquery(
            self,
            vector: list[float],
            score_threshold: float = 0.7,
            limit: int = 10
    ):
        res = await self.qdrant_client.query_points(
            collection_name=self.collection_name,
            query=vector,
            score_threshold=score_threshold,
            limit=limit
        )

        return [ point.payload for point in res.points ]

