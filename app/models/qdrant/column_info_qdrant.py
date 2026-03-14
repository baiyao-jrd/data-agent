"""
------------------------------------------------
    @Time: 2026/3/14 20:32 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/column_info_qdrant.py
    @Software: PyCharm
    @Description:
------------------------------------------------
"""
from typing import TypedDict


class ColumnInfoQdrant(TypedDict):
    id: str
    name: str
    role: str
    examples: list
    description: str
    alias: list
    table_id: str