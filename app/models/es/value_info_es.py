"""
------------------------------------------------
    @Time: 2026/3/15 10:46 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/value_info_es.py
    @Software: PyCharm
    @Description:
------------------------------------------------
"""
from typing import TypedDict

class ValueInfoEs(TypedDict):
    """ 实际上 列名、表名等信息可以通过 column_id, table_id 进行查询，但这里也直接进行存储 """
    id: str
    value: str
    type: str
    column_id: str
    column_name: str
    table_id: str
    table_name: str

