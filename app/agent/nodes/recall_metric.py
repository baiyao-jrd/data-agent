"""
------------------------------------------------
    @Time: 2026/3/16 16:56 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/recall_metric.py
    @Software: PyCharm
    @Description: 【指标信息召回】节点
------------------------------------------------
"""
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime

from agent.context import DataAgentContext
from agent.llm import llm
from agent.state import DataAgentState
from core.logger import logger
from models.qdrant.metric_info_qdrant import MetricInfoQdrant
from prompt.prompt_loader import load_prompt

async def recall_metric(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("✅指标信息召回")

    embedding_client = runtime.context['embedding_client'] # 向量化客户端
    metric_qdrant_repository = runtime.context['metric_qdrant_repository'] # 指标信息对应的qdrant持久层

    """ 1. 使用llm扩展关键词 """
    query = state['query']
    keywords = state['keywords']

    prompt = PromptTemplate(
        template=load_prompt("extend_keywords_for_metric_recall"),
        input_variables=["query"]
    )
    output_parser = JsonOutputParser()

    chain = prompt | llm | output_parser

    expanded_keywords = await chain.ainvoke(
        input={
            "query": query,
        }
    )

    # 扩展后合并
    keywords = list(set(keywords + expanded_keywords))

    logger.info(f"经扩展后的关键字列表为：{keywords}")

    """ 2. 使用扩展后的关键词召回指标信息 """
    retrieved_metrics_map: dict[str, MetricInfoQdrant] = {}

    for keyword in keywords: # 逐个关键词进行查询
        vector = await embedding_client.aembed_query(keyword)
        pay_loads: list[MetricInfoQdrant] = await metric_qdrant_repository.aquery(
            vector = vector,
        )

        for payload in pay_loads:
            metric_id = payload['id']
            if not retrieved_metrics_map.get(metric_id):
                retrieved_metrics_map[metric_id] = payload

    logger.info(f"召回的指标信息为：{list(retrieved_metrics_map.keys())}")

    return {
        "retrieved_metrics": list(retrieved_metrics_map.values())
    }


