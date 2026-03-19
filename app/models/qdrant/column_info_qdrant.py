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
from typing import TypedDict, Optional


class ColumnInfoQdrant(TypedDict):
    id: str
    name: Optional[str]
    type: Optional[str]
    role: Optional[str]
    examples: Optional[dict | list]
    description: Optional[str]
    alias: Optional[dict | list]
    table_id: Optional[str]