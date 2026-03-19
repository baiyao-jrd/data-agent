"""
------------------------------------------------
    @Time: 2026/3/15 10:05 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/value_es_repository.py
    @Software: PyCharm
    @Description:
------------------------------------------------
"""
from elasticsearch import AsyncElasticsearch

from app.models.es.value_info_es import ValueInfoEs


class ValueEsRepository:
    index_name = "data-agent-value"
    index_mapping = {
        "dynamic": False,
        "properties": {
            "id": {
                "type": "keyword"
            },
            "value": { # 读写时用的均为常用的中文分词器 -> ik_max_word
                "type": "text",  # 只要进行全文检索, 类型就需要是 text
                "analyzer": "ik_max_word",
                "search_analyzer": "ik_max_word"
            },
            "type": {
                "type": "keyword"
            },
            "column_id": {
                "type": "keyword"
            },
            "column_name": {
                "type": "keyword"
            },
            "table_id": {
                "type": "keyword"
            },
            "table_name": {
                "type": "keyword"
            },
        }
    }

    def __init__(self, es_client: AsyncElasticsearch):
        self.es_client = es_client

    async def ensure_create_index(self):
        """
            确保索引存在 -> 参考：https://www.elastic.co/guide/en/elasticsearch/reference/8.19/getting-started.html
        """
        if not await self.es_client.indices.exists(index=self.index_name):
            resp = await self.es_client.indices.create(  # 显示声明索引结构 -> 该处索引的概念类似于表
                index=self.index_name,
                mappings=self.index_mapping,
            )

    async def index(self, value_infos: list[ValueInfoEs], batch_size=20):
        for i in range(0, len(value_infos), batch_size):
            batch = value_infos[i:i + batch_size]
            operations = []
            for value_info in batch: # 一条操作一条数据的插入
                operations.append({
                    "index": {
                        "_index": self.index_name,
                        "_id": value_info["id"]
                    }
                })
                operations.append(value_info)

            await self.es_client.bulk(operations=operations)

    async def aquery(self, keyword: str, threshold: float = 0.6, limit: int = 5 ):
        res = await self.es_client.search(
            index=self.index_name,
            query={
                "match": {
                    "value": keyword
                }
            },
            min_score=threshold,
            size=limit
        )

        await self.es_client.close()

        return [hit["_source"] for hit in res["hits"]["hits"]]

