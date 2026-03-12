"""
------------------------------------------------
    @Time: 2026/3/12 15:38 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/es_client_manager.py
    @Software: PyCharm
    @Description: ElasticSearch 客户端
------------------------------------------------
"""
import asyncio
from typing import Optional

from conf.app_config import ESConfig, app_config
from elasticsearch import AsyncElasticsearch

class EsClientManager:
    def __init__(self, es_config: ESConfig):
        self.es_config = es_config
        self.client: Optional[AsyncElasticsearch] = None

    def get_url(self):
        url = f"http://{self.es_config.host}:{self.es_config.port}"
        return url

    def init(self):
        self.client = AsyncElasticsearch(
            hosts = [self.get_url()]
        )

    async def close(self):
        await self.client.close()

es_client_manager = EsClientManager(app_config.es)

async def test():
    es_client_manager.init()
    client = es_client_manager.client

    # TODO 1. 显式创建索引
    await client.indices.create(
        index="my_books",
        mappings={
            "dynamic": False,
            "properties": {
                "name": {
                    "type": "text"
                },
                "author": {
                    "type": "text"
                },
                "release_date": {
                    "type": "date",
                    "format": "yyyy-MM-dd"
                },
                "page_count": {
                    "type": "integer"
                }
            }
        },
    )

    # TODO 2. 批量插入数据
    await client.bulk(
        operations=[
            {
                "index": {
                    "_index": "my_books"
                }
            },
            {
                "name": "Revelation Space",
                "author": "Alastair Reynolds",
                "release_date": "2000-03-15",
                "page_count": 585
            },
            {
                "index": {
                    "_index": "my_books"
                }
            },
            {
                "name": "1984",
                "author": "George Orwell",
                "release_date": "1985-06-01",
                "page_count": 328
            },
            {
                "index": {
                    "_index": "my_books"
                }
            },
            {
                "name": "Fahrenheit 451",
                "author": "Ray Bradbury",
                "release_date": "1953-10-15",
                "page_count": 227
            },
            {
                "index": {
                    "_index": "my_books"
                }
            },
            {
                "name": "Brave New World",
                "author": "Aldous Huxley",
                "release_date": "1932-06-01",
                "page_count": 268
            },
            {
                "index": {
                    "_index": "my_books"
                }
            },
            {
                "name": "The Handmaids Tale",
                "author": "Margaret Atwood",
                "release_date": "1985-06-01",
                "page_count": 311
            }
        ],
    )

    # TODO 3. 查询数据 -> 全文检索
    resp = await client.search(
        index="my_books",
        query={
            "match": {
                "name": "brave"
            }
        },
    )
    print(resp)

    await client.close()

if __name__ == '__main__':
    asyncio.run(test())

""" 输出
{
  'took': 6,
  'timed_out': False,
  '_shards': {
    'total': 1,
    'successful': 1,
    'skipped': 0,
    'failed': 0
  },
  'hits': {
    'total': {
      'value': 1,
      'relation': 'eq'
    },
    'max_score': 1.2067741,
    'hits': [{
      '_index': 'my_books',
      '_id': 's5Qo4ZwBdUZamlOwZV56',
      '_score': 1.2067741,
      '_source': {
        'name': 'Brave New World',
        'author': 'Aldous Huxley',
        'release_date': '1932-06-01',
        'page_count': 268
      }
    }]
  }
}
"""