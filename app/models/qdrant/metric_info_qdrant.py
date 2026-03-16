"""
------------------------------------------------
    @Time: 2026/3/16 11:50 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/metric_info_qdrant.py
    @Software: PyCharm
    @Description:
------------------------------------------------
"""
from typing import TypedDict


class MetricInfoQdrant(TypedDict):
    id: str
    name: str
    description: str
    relevant_columns: list
    alias: list