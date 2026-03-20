"""
------------------------------------------------
    @Time: 2026/3/13 19:11 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/meta_mysql_repository.py
    @Software: PyCharm
    @Description:
------------------------------------------------
"""
from typing import Optional

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.mysql.column_metric_mysql import ColumnMetricMySQL
from app.models.mysql.metric_info_mysql import MetricInfoMySQL
from models.mysql.column_info_mysql import ColumnInfoMySQL
from models.mysql.table_info_mysql import TableInfoMySQL


class MetaMysqlRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def save_tables_info(self, tables_info):
        self.session.add_all(tables_info)

    async def save_columns_info(self, columns_info):
        self.session.add_all(columns_info)

    async def save_metric_infos(self, metric_infos: list[MetricInfoMySQL]):
        self.session.add_all(metric_infos)

    async def save_column_metric_infos(self, column_metric_infos: list[ColumnMetricMySQL]):
        self.session.add_all(column_metric_infos)

    async def get_column_info_by_id(self, column_id: str) -> Optional[ColumnInfoMySQL]:
        return await self.session.get( # 使用封装好的方法，直接通过属性ID获取数据
            entity=ColumnInfoMySQL,
            ident=column_id
        )

    async def get_table_info_by_id(self, table_id: str) -> Optional[TableInfoMySQL]:
        return await self.session.get( # 使用封装好的方法，直接通过表ID获取数据
            entity=TableInfoMySQL,
            ident=table_id
        )

    async def get_key_columns_info_by_table_id(self, table_id: str) -> list[ColumnInfoMySQL]:
        sql = """
            select
                *
            from column_info
            where table_id = :table_id
                and type in ('primary_key', 'foreign_key')
        """

        query = select(ColumnInfoMySQL).from_statement(text(sql))

        res = await self.session.execute(
            statement=query,
            params={"table_id": table_id}
        )

        return list(res.scalars().fetchall())