"""
------------------------------------------------
    @Time: 2026/3/11 19:58 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/qdrant_client_manager.py
    @Software: PyCharm
    @Description: qdrant 客户端
------------------------------------------------
"""
import asyncio
import random
from typing import Optional

import numpy as np
from conf.app_config import QdrantConfig, app_config
from qdrant_client import AsyncQdrantClient, models


class QdrantClientManager:
    def __init__(self, qdrant_config: QdrantConfig):
        self.qdrant_config = qdrant_config
        self.client: Optional[AsyncQdrantClient] = None

    def get_url(self):
        url = f"http://{self.qdrant_config.host}:{self.qdrant_config.port}"
        return url

    def init(self):
        self.client = AsyncQdrantClient(
            url=self.get_url(),
            timeout=30
        )

    async def close(self):
        await self.client.close()

qdrant_client_manager = QdrantClientManager(app_config.qdrant)

async def test():
    qdrant_client_manager = QdrantClientManager(app_config.qdrant)
    qdrant_client_manager.init()
    client = qdrant_client_manager.client

    if not await client.collection_exists("my_collection"):
        await client.create_collection(
            collection_name="my_collection",
            vectors_config=models.VectorParams(size=10, distance=models.Distance.COSINE),
        )

    await client.upsert(
        collection_name="my_collection",
        points=[
            models.PointStruct(
                id=i,
                vector=[ random.random() for _ in range(10) ],
            )
            for i in range(100)
        ],
    )

    res = await client.query_points(
        collection_name="my_collection",
        query=[ random.random() for _ in range(10) ],  # type: ignore
        limit=10,
        # score_threshold=0.9 # 可设置阈值，只有大于该阈值的点才会返回
    )

    print(res)

if __name__ == '__main__':
    asyncio.run(test())

""" 查询出相似的10个点
points=[
    ScoredPoint(id=25, version=1, score=0.96407056, payload={}, vector=None, shard_key=None, order_value=None), 
    ScoredPoint(id=15, version=1, score=0.9577742, payload={}, vector=None, shard_key=None, order_value=None), 
    ScoredPoint(id=83, version=1, score=0.93954706, payload={}, vector=None, shard_key=None, order_value=None), 
    ScoredPoint(id=11, version=1, score=0.9259665, payload={}, vector=None, shard_key=None, order_value=None), 
    ScoredPoint(id=40, version=1, score=0.9235002, payload={}, vector=None, shard_key=None, order_value=None), 
    ScoredPoint(id=23, version=1, score=0.901871, payload={}, vector=None, shard_key=None, order_value=None), 
    ScoredPoint(id=65, version=1, score=0.8904275, payload={}, vector=None, shard_key=None, order_value=None), 
    ScoredPoint(id=36, version=1, score=0.88722265, payload={}, vector=None, shard_key=None, order_value=None), 
    ScoredPoint(id=77, version=1, score=0.8855326, payload={}, vector=None, shard_key=None, order_value=None), 
    ScoredPoint(id=2, version=1, score=0.88179, payload={}, vector=None, shard_key=None, order_value=None)
]
"""