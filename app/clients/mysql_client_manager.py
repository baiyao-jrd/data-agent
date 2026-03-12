"""
------------------------------------------------
    @Time: 2026/3/11 12:55 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/mysql_client_manager.py
    @Software: PyCharm
    @Description: Mysql 客户端
------------------------------------------------
"""
import asyncio
from typing import Optional

from sqlalchemy import text

from conf.app_config import app_config, DBConfig
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncSession, async_sessionmaker

class MysqlClientManager:
    def __init__(self, db_config: DBConfig):
        self.engine : Optional[AsyncEngine] = None
        self.db_config = db_config
        self.session_factory = None

    def get_url(self):
        url = f"mysql+asyncmy://{self.db_config.user}:{self.db_config.password}@{self.db_config.host}:{self.db_config.port}/{self.db_config.database}?charset=utf8mb4"
        return url

    def init(self):
        self.engine = create_async_engine(
            url = self.get_url(),
            pool_size = 10,
            pool_pre_ping = True,
        )
        self.session_factory = async_sessionmaker(
            bind = dw_mysql_client_manager.engine,
            autoflush = True, # 查询前自动flush，防止与数据库数据不一致
            expire_on_commit = False # 如果设置为True, commit之后，属性失效，异步情况下势必大概率会重新查询属性，此时报错，为避免出现异常，这里设置为False
        )

    def close(self):
        self.engine.dispose()

meta_mysql_client_manager = MysqlClientManager(app_config.db_meta)
dw_mysql_client_manager = MysqlClientManager(app_config.db_dw)

if __name__ == '__main__':
    dw_mysql_client_manager.init()

    async def test():
        async with dw_mysql_client_manager.session_factory() as session: # session_factory 加() 之后创建一个新的 session 对象
            result = await session.execute(text("select * from fact_order"))
            print(result.mappings().fetchone().order_id)

    asyncio.run(test())