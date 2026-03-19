"""
------------------------------------------------
    @Time: 2026/3/16 16:55 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/recall_column.py
    @Software: PyCharm
    @Description: 【字段信息召回】节点
------------------------------------------------
"""
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime


from agent.context import DataAgentContext
from agent.llm import llm
from agent.state import DataAgentState
from core.logger import logger
from models.qdrant.column_info_qdrant import ColumnInfoQdrant
from prompt.prompt_loader import load_prompt


async def recall_column(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("✅字段信息召回")

    """ 1. 定义llm chain用于扩展关键字 """

    prompt = PromptTemplate(
        template=load_prompt("extend_keywords_for_column_recall"), # 仅传入文件名 -> 不包含后缀
        input_variables=["query"]
    )
    output_parser = JsonOutputParser()

    chain = prompt | llm | output_parser

    query = state["query"]
    keywords = state["keywords"]

    expanded_keywords = await chain.ainvoke(
        input={
            "query": query,
        }
    )

    # 扩展后合并
    keywords = list(set(keywords + expanded_keywords))

    logger.info(f"经扩展后的关键字列表为：{keywords}")

    """ 2. 使用扩展后的关键字进行字段信息召回 """
    """
        思路：先将关键词列表进行向量化，然后拿着向量匹配Qdrant
        注意：将所需的客户端依赖均存储在 Runtime[DataAgentContext] 中
    """
    retrieved_columns_map: dict[str, ColumnInfoQdrant] = {} # 最后召回的字段信息 -> id作为key, ColumnInfoQdrant作为value -> 便于去重

    embedding_client = runtime.context["embedding_client"]
    column_qdrant_repository = runtime.context["column_qdrant_repository"]

    for keyword in keywords: # 逐个关键词进行查询
        vector = await embedding_client.aembed_query(keyword)
        pay_loads: list[ColumnInfoQdrant] = await column_qdrant_repository.aquery(
            vector = vector,
        )

        # 注意：召回出的字段信息中，可能存在重复的字段，所以需要进行去重 -> 表名.列名 作为ID -> 直接使用id作为去重条件
        for payload in pay_loads:
            column_id = payload['id']
            if not retrieved_columns_map.get(column_id):
                retrieved_columns_map[column_id] = payload

    logger.info(f"召回的字段信息为：{list(retrieved_columns_map.keys())}")

    return { # 后续需要对召回字段进行合并处理，所以需要将结果放回状态中以便后续操作
        "retrieved_columns": list(retrieved_columns_map.values())
    }




