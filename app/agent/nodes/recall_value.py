"""
------------------------------------------------
    @Time: 2026/3/16 16:57 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/recall_value.py
    @Software: PyCharm
    @Description: 【字段值召回】节点
------------------------------------------------
"""
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime

from agent.context import DataAgentContext
from agent.llm import llm
from agent.state import DataAgentState
from core.logger import logger
from models.es.value_info_es import ValueInfoEs
from prompt.prompt_loader import load_prompt


async def recall_value(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("✅字段值召回")

    query = state['query']
    keywords = state['keywords'] # 一开始抽取的关键字列表

    value_es_repository = runtime.context['value_es_repository']

    """ 1. 使用llm扩展关键字信息 """
    prompt = PromptTemplate(
        template=load_prompt("extend_keywords_for_value_recall"),
        input_variables=["query"]
    )
    output_parser = JsonOutputParser()

    chain = prompt | llm | output_parser

    expanded_keywords = await chain.ainvoke(
        input={
            "query": query,
        }
    )

    """" 2. 使用扩展后的关键字信息召回字段取值 """
    keywords = list(set(keywords + expanded_keywords))
    values_map: dict[str, ValueInfoEs] = {} # 语义相近的关键字可能会召回相同的值信息, 所以需要去重

    for key_word in keywords:
        value_infos: list[ValueInfoEs] = await value_es_repository.aquery(
            keyword=key_word
        )
        for val_info in value_infos:
            val_id = val_info['id']
            if not values_map.get(val_id):
                values_map[val_id] = val_info

    retrieved_values = list(values_map.values())

    logger.info(f"✋🏻 召回字段取值：{values_map.keys()}")

    return {
        "retrieved_values": retrieved_values
    }




