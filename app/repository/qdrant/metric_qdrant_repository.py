"""
------------------------------------------------
    @Time: 2026/3/16 14:19 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/metric_qdrant_repository.py
    @Software: PyCharm
    @Description:
------------------------------------------------
"""
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct

from app.conf.app_config import app_config
from app.models.qdrant.metric_info_qdrant import MetricInfoQdrant


class MetricQdrantRepository:

    collection_name = "data-agent-metric"

    def __init__(self, client: AsyncQdrantClient):
        self.client = client

    async def ensure_create_collection(self): # 确保事先创建collection
        if not await self.client.collection_exists(self.collection_name):
            await self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(
                    size=app_config.qdrant.embedding_size,
                    distance=Distance.COSINE
                )
            )

    async def upsert_points(
            self,
            ids: list[str],
            vectors: list[list[float]],
            payloads: list[MetricInfoQdrant],
            batch_size: int = 20 # 按批对client发起请求 -> 一次发起请求数不宜太多
    ):
        zipped = list(zip(ids, vectors, payloads)) # zip之后是一个迭代器, 需转为列表后才能进行切片
        for i in range(0, len(zipped), batch_size):
            batch = zipped[i:i+batch_size] # 每个元素是一个三元组
            points = [ PointStruct(
                id=id,
                vector=embedding,
                payload=payload
            ) for id, embedding, payload in batch ]
            await self.client.upsert(
                collection_name=self.collection_name,
                points=points
            )

    async def aquery(
            self,
            vector: list[float],
            score_threshold: float = 0.7,
            limit: int = 10
    ) -> list[MetricInfoQdrant]:
        res = await self.client.query_points(
            collection_name=self.collection_name,
            query=vector,
            score_threshold=score_threshold,
            limit=limit
        )

        return [ point.payload for point in res.points ]