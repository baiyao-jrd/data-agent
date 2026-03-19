"""
------------------------------------------------
    @Time: 2026/3/16 16:58 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/merge_retrieved_info.py
    @Software: PyCharm
    @Description: 【合并召回信息】节点
------------------------------------------------
"""
from langgraph.runtime import Runtime

from agent.context import DataAgentContext
from agent.state import DataAgentState, TableRecallInfoState, MetricRecallInfoState
from models.mysql.column_info_mysql import ColumnInfoMySQL
from models.qdrant.column_info_qdrant import ColumnInfoQdrant


async def merge_retrieved_info(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("✅合并召回信息")

    """ 1. 定义最终合并后的信息列表 """
    table_infos: list[TableRecallInfoState] = []
    metric_infos: list[MetricRecallInfoState] = []

    """ 2. 取出状态中的字段、指标、值信息用于合并信息的组装 """
    retrieved_columns = state['retrieved_columns']
    retrieved_values = state['retrieved_values']
    retrieved_metrics = state['retrieved_metrics']

    """ 3. 🌟将 retrieved_metrics 中的 relevant_columns 字段信息加入到 retrieved_columns 中 """
    """ 3.1 获取所需依赖 """
    meta_mysql_repository = runtime.context['meta_mysql_repository']

    """ 3.2 将指标信息的 relevant_columns 加入到字段信息列表 """
    # 字典结构 -> 用于召回字段去重
    retrieved_columns_map: dict[str, ColumnInfoQdrant] = {column['id']: column for column in retrieved_columns}
    for metric in retrieved_metrics:
        for column_id in metric['relevant_columns']: # fact_order.order_quantity -> 此为 column_info.id 列id
            if column_id not in retrieved_columns_map:
                column_info_mysql: ColumnInfoMySQL = await meta_mysql_repository.get_column_info_by_id(column_id=column_id)
                # 注意：relevant_columns 与 召回字段 可能存在重复，需要去重, 同时需要将 ColumnInfoMySQL 转化为 ColumnInfoQdrant
                column_info_qdrant = await convert_column_info_from_mysql_to_qdrant(column_info_mysql)
                retrieved_columns_map[column_id] = column_info_qdrant

    return {
        "table_infos": table_infos,
        "metric_infos": metric_infos
    }

async def convert_column_info_from_mysql_to_qdrant(column_info_mysql: ColumnInfoMySQL) -> ColumnInfoQdrant:
    return {
        "id": column_info_mysql.id,
        "name": column_info_mysql.name,
        "type": column_info_mysql.type,
        "role": column_info_mysql.role,
        "examples": column_info_mysql.examples,
        "description": column_info_mysql.description,
        "alias": column_info_mysql.alias,
        "table_id": column_info_mysql.table_id
    }