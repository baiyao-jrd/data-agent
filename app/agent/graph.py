"""
------------------------------------------------
    @Time: 2026/3/16 15:33
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/graph.py
    @Software: PyCharm
    @Description:
------------------------------------------------
"""
import asyncio
import logging
import os
import jieba

from langgraph.constants import START, END
from langgraph.graph import StateGraph

from agent.context import DataAgentContext
from agent.nodes.add_extra_context import add_extra_context
from agent.nodes.correct_sql import correct_sql
from agent.nodes.execute_sql import execute_sql
from agent.nodes.extract_keywords import extract_keywords
from agent.nodes.filter_metric import filter_metric
from agent.nodes.filter_table import filter_table
from agent.nodes.generate_sql import generate_sql
from agent.nodes.merge_retrieved_info import merge_retrieved_info
from agent.nodes.recall_column import recall_column
from agent.nodes.recall_metric import recall_metric
from agent.nodes.recall_value import recall_value
from agent.nodes.validate_sql import validate_sql
from agent.state import DataAgentState
from clients.embedding_client_manager import embedding_client_manager
from clients.es_client_manager import es_client_manager
from clients.qdrant_client_manager import qdrant_client_manager
from repository.es.value_es_repository import ValueEsRepository
from repository.qdrant.column_qdrant_repository import ColumnQdrantRepository
from repository.qdrant.metric_qdrant_repository import MetricQdrantRepository

""" 0. 日志信息设置 """
# 压制 gRPC / absl 输出
os.environ["GRPC_VERBOSITY"] = "NONE"

# 压制 jieba 初始化输出
jieba.setLogLevel(logging.ERROR)

graph_builder = StateGraph(
    state_schema=DataAgentState,
    context_schema=DataAgentContext,
)

""" 1. 添加节点 """
graph_builder.add_node("extract_keywords", extract_keywords)
graph_builder.add_node("recall_column", recall_column)
graph_builder.add_node("recall_metric", recall_metric)
graph_builder.add_node("recall_value", recall_value)
graph_builder.add_node("merge_retrieved_info", merge_retrieved_info)
graph_builder.add_node("filter_table", filter_table)
graph_builder.add_node("filter_metric", filter_metric)
graph_builder.add_node("add_extra_context", add_extra_context)
graph_builder.add_node("generate_sql", generate_sql)
graph_builder.add_node("validate_sql", validate_sql)
graph_builder.add_node("correct_sql", correct_sql)
graph_builder.add_node("execute_sql", execute_sql)

""" 2. 添加关系 """
graph_builder.add_edge(START, "extract_keywords")
graph_builder.add_edge("extract_keywords", "recall_column")
graph_builder.add_edge("extract_keywords", "recall_metric")
graph_builder.add_edge("extract_keywords", "recall_value")
graph_builder.add_edge("recall_column", "merge_retrieved_info")
graph_builder.add_edge("recall_metric", "merge_retrieved_info")
graph_builder.add_edge("recall_value", "merge_retrieved_info")
graph_builder.add_edge("merge_retrieved_info", "filter_table")
graph_builder.add_edge("merge_retrieved_info", "filter_metric")
graph_builder.add_edge("filter_table", "add_extra_context")
graph_builder.add_edge("filter_metric", "add_extra_context")
graph_builder.add_edge("add_extra_context", "generate_sql")
graph_builder.add_edge("generate_sql", "validate_sql")

graph_builder.add_conditional_edges(
    "validate_sql",
    lambda state: "execute_sql" if state['error'] is None else "correct_sql",
    {
        "execute_sql": "execute_sql",
        "correct_sql": "correct_sql",
    }
)

graph_builder.add_edge("correct_sql", "execute_sql")
graph_builder.add_edge("execute_sql", END)

""" 3. 编译图 """
graph = graph_builder.compile()

# print(graph.get_graph().draw_mermaid())

if __name__ == '__main__':
    async def test():
        state = DataAgentState(query="统计华北地区的销售总额", error="")

        embedding_client_manager.init()
        qdrant_client_manager.init()
        es_client_manager.init()

        column_qdrant_repository = ColumnQdrantRepository(
            qdrant_client=qdrant_client_manager.client
        )
        value_es_repository = ValueEsRepository(
            es_client=es_client_manager.client
        )
        metric_qdrant_repository = MetricQdrantRepository(
            client=qdrant_client_manager.client
        )
        context = DataAgentContext(
            embedding_client=embedding_client_manager.client,
            column_qdrant_repository=column_qdrant_repository,
            value_es_repository=value_es_repository,
            metric_qdrant_repository=metric_qdrant_repository,
        )
        async for chunk in graph.astream(
                input=state,
                context=context,
                stream_mode="custom",
        ):
            print(chunk)

    asyncio.run(test())

""" output
2026-03-19 09:44:31.781 | INFO     | request_id - c316a5f4-2aae-4852-ab1c-96def91abf3e | agent.nodes.extract_keywords:extract_keywords:51 
    - ✋🏻查询的关键字为：['销售总额', '统计', '统计华北地区的销售总额', '华北地区']
2026-03-19 09:44:33.121 | INFO     | request_id - 96f6c904-617b-4402-8fd4-bdb00898df21 | agent.nodes.recall_value:recall_value:62 
    - ✋🏻 召回字段取值：dict_keys(['dim_region.region_name.华北'])
2026-03-19 09:44:33.700 | INFO     | request_id - e1463260-02ed-416f-8f53-a8efc6c86b3d | agent.nodes.recall_column:recall_column:50 
    - 经扩展后的关键字列表为：['销售总额', '统计华北地区的销售总额', '销售额', '统计', '地区', '统计时间', '华北地区']
2026-03-19 09:44:37.963 | INFO     | request_id - 51790f62-17f8-4911-95fc-77fecb589f70 | agent.nodes.recall_column:recall_column:74 
    - 召回的字段信息为：['fact_order.order_amount', 'fact_order.order_quantity', 'dim_region.region_name', 'dim_region.province', 'dim_region.country', 'dim_region.region_id', 'fact_order.region_id']
"""