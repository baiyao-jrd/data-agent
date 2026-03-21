"""
------------------------------------------------
    @Time: 2026/3/14 11:09 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/dw_mysql_repository.py
    @Software: PyCharm
    @Description:
------------------------------------------------
"""
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from agent.state import DbInfoState


class DwMysqlRepository:
    def __init__(self, session: AsyncSession): # 只有持有 session 才能进行读写操作
        self.session = session

    async def get_column_types(self, table_name: str):
        sql = f"show columns from {table_name}"
        res = await self.session.execute(text(sql))
        return {row.Field: row.Type for row in res.fetchall()} # 返回字典 {字段名: 字段类型}

    async def get_column_values(self, table_name, column_name, limit):
        sql = f"select distinct {column_name} from {table_name} limit {limit}" # 去重 -> 不要保存重复的值
        res = await self.session.execute(text(sql))
        # return [row[0] for row in res.fetchall()]
        return res.scalars().all() # 效果等价于上面的语句

    async def get_db_info(self):
        sql = "select version()"
        version = (await self.session.execute(text(sql))).scalar()
        dialect = self.session.get_bind().dialect.name

        return DbInfoState(
            dialect=dialect,
            version=version
        )

    async def validate_sql(self, statement: str):
        await self.session.execute(text(f"explain {statement}"))