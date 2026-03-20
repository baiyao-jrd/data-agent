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
from agent.state import DataAgentState, TableRecallInfoState, MetricRecallInfoState, ColumnRecallInfoState
from core.logger import logger
from models.mysql.column_info_mysql import ColumnInfoMySQL
from models.mysql.table_info_mysql import TableInfoMySQL
from models.qdrant.column_info_qdrant import ColumnInfoQdrant
from models.qdrant.metric_info_qdrant import MetricInfoQdrant


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

    """ 4. 🌟将值信息添加至retrieved_columns的 examples 中 """
    """ 思路分析
    ① 值字段存在，则直接将值添加至examples中
    ② 值所属字段不在 retrieved_columns 中，则需要查库将字段信息放至 retrieved_columns 中，并将值信息也放至 examples 中
    """
    for val_info_es in retrieved_values:
        column_id = val_info_es['column_id']
        column_value = val_info_es['value']

        """ 判断：如果当前值所属字段是不存在于 召回字段列表 中，需事先添加 """
        if column_id not in retrieved_columns_map:
            column_info_mysql: ColumnInfoMySQL = await meta_mysql_repository.get_column_info_by_id(column_id=column_id)
            column_info_qdrant = await convert_column_info_from_mysql_to_qdrant(
                column_info_mysql
            )
            retrieved_columns_map[column_id] = column_info_qdrant

        example_values: list[str] = retrieved_columns_map[column_id]['examples']
        if column_value not in example_values:  # 判断一下：仅在值不存在的时候再添加
            example_values.append(column_value)

    """ 5. 🌟将字段信息按照所属表进行整理，得到最终的 table_infos: list[TableRecallInfoState] = [] """
    """ 思路分析
    先使用字典将字段进行分组：key为表id，value为属于同一张表的所有字段
    接着由 table_id 查出表信息，再结合表下面的列信息就可以直接构造 TableRecallInfoState
    """
    """ ① dict[str, list[ColumnInfoQdrant]] -> table: columns 映射 """
    columns_group_by_table_id: dict[str, list[ColumnInfoQdrant]] = {} # key为表id，value为表属所有column信息

    for column_info_qdrant in retrieved_columns_map.values():
        table_id = column_info_qdrant['table_id']

        if table_id not in columns_group_by_table_id: # 键中不含该表，先进行初始化
            columns_group_by_table_id[table_id] = []

        columns_group_by_table_id[table_id].append(column_info_qdrant)

    """ 🌟此处增加优化措施 -> 显式增加表的主外键信息，思路如下：
        遍历 columns_group_by_table_id 字典的key -> 查库表主外键，然后确保
        在召回字段列表 list[ColumnInfoQdrant] 中不存在的情况下再添加至列表中
    """
    for table_id in columns_group_by_table_id.keys():
        key_columns: list[ColumnInfoMySQL] = await meta_mysql_repository.get_key_columns_info_by_table_id(table_id=table_id)

        columns_id = [column_info_qdrant['id'] for column_info_qdrant in columns_group_by_table_id[table_id]]
        for column_info_mysql in key_columns:
            column_info_qdrant: ColumnInfoQdrant = await convert_column_info_from_mysql_to_qdrant(column_info_mysql)
            if column_info_qdrant['id'] not in columns_id:
                columns_group_by_table_id[table_id].append(column_info_qdrant)

    """ ② table: columns 映射 => 转换为 TableRecallInfoState 并添加至 table_infos 中 """
    for table_id, columns in columns_group_by_table_id.items():
        table_info: TableInfoMySQL = await meta_mysql_repository.get_table_info_by_id(table_id=table_id)

        columns_recall_info_state: list[ColumnRecallInfoState] = [
            await convert_column_info_from_qdrant_to_state(column_info_qdrant) for column_info_qdrant in columns
        ]

        table_recall_info_state: TableRecallInfoState = TableRecallInfoState(
            name=table_info.name,
            role=table_info.role,
            description=table_info.description,
            columns=columns_recall_info_state
        )

        table_infos.append(table_recall_info_state)

    """ 6. MetricInfoQdrant 转换为 MetricRecallInfoState 并得到 metric_infos: list[MetricRecallInfoState] """
    metric_infos: list[MetricRecallInfoState] = [
        await convert_metric_info_from_qdrant_to_state(metric_info_qdrant=metric_info_qdrant) for metric_info_qdrant in retrieved_metrics
    ]

    # 日志
    logger.info(
        f"✅合并召回信息完成: 表信息 -> {[ table_info.get('name') for table_info in table_infos ]}, 指标信息 -> {[ metric_info.get('name') for metric_info in metric_infos ]}"
    )

    return {
        "table_infos": table_infos,
        "metric_infos": metric_infos
    }

async def convert_metric_info_from_qdrant_to_state(metric_info_qdrant: MetricInfoQdrant):
    return MetricRecallInfoState(
        name=metric_info_qdrant['name'],
        description=metric_info_qdrant['description'],
        relevant_columns=metric_info_qdrant['relevant_columns'],
        alias=metric_info_qdrant['alias']
    )

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

async def convert_column_info_from_qdrant_to_state(column_info_qdrant: ColumnInfoQdrant) -> ColumnRecallInfoState:
    return ColumnRecallInfoState(
        name=column_info_qdrant['name'],
        type=column_info_qdrant['type'],
        role=column_info_qdrant['role'],
        examples=column_info_qdrant['examples'],
        description=column_info_qdrant['description'],
        alias=column_info_qdrant['alias']
    )