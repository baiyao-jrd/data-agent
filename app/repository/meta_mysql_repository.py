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


class MetaMysqlRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def save_tables_info(self, tables_info):
        self.session.add_all(tables_info)

    async def save_columns_info(self, columns_info):
        self.session.add_all(columns_info)