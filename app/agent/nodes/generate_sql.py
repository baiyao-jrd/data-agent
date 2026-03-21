"""
------------------------------------------------
    @Time: 2026/3/16 17:03 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/generate_sql.py
    @Software: PyCharm
    @Description: 【生成SQL】节点
------------------------------------------------
"""
import yaml
from langchain_core.output_parsers import JsonOutputParser, StrOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime

from agent.context import DataAgentContext
from agent.llm import llm
from agent.state import DataAgentState
from core.logger import logger
from prompt.prompt_loader import load_prompt


async def generate_sql(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("✅生成SQL")

    """ 1. 上下文中获取组装prompt所需信息 """
    query = state["query"]
    table_infos = state["table_infos"]
    metric_infos = state["metric_infos"]
    date_info = state["date_info"]
    db_info = state["db_info"]

    """ 2. 调用llm chain生成SQL """
    prompt = PromptTemplate(
        template=load_prompt("generate_sql"),
        input_variables=["query", "table_infos", "metric_infos", "date_info", "db_info"]
    )
    output_parser = StrOutputParser()
    chain = prompt | llm | output_parser
    sql = await chain.ainvoke(
        {
            "query": query,
            "table_infos": yaml.dump(table_infos, allow_unicode=True, sort_keys=False),
            "metric_infos": yaml.dump(metric_infos, allow_unicode=True, sort_keys=False),
            "date_info": yaml.dump(date_info, allow_unicode=True, sort_keys=False),
            "db_info": yaml.dump(db_info, allow_unicode=True, sort_keys=False)
        }
    )

    """ 3. 作为状态返回 """
    logger.info(f"\n生成的SQL: \n{sql}")
    return {
        "sql": sql
    }

""" output 示例
生成的SQL: 

SELECT SUM(fo.order_amount) AS 销售总额
FROM fact_order fo
JOIN dim_region dr ON fo.region_id = dr.region_id
WHERE dr.region_name = '华北';
"""