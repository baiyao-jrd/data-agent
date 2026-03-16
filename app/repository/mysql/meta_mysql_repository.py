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
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.mysql.column_metric_mysql import ColumnMetricMySQL
from app.models.mysql.metric_info_mysql import MetricInfoMySQL


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