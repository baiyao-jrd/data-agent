"""
------------------------------------------------
    @Time: 2026/3/16 15:33 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/state.py
    @Software: PyCharm
    @Description:
------------------------------------------------
"""
from typing import TypedDict

from models.es.value_info_es import ValueInfoEs
from models.qdrant.column_info_qdrant import ColumnInfoQdrant
from models.qdrant.metric_info_qdrant import MetricInfoQdrant


class DataAgentState(TypedDict):
    query: str # 用户输入的查询
    keywords: list[str] # 从用户查询中提取的关键字
    error: str # SQL校验时的错误信息
    retrieved_columns: list[ColumnInfoQdrant] # 召回的字段信息
    retrieved_values: list[ValueInfoEs] # 召回的字段值信息
    retrieved_metrics: list[MetricInfoQdrant] # 召回的指标信息