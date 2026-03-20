"""
------------------------------------------------
    @Time: 2026/3/16 17:00 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/filter_table.py
    @Software: PyCharm
    @Description: 【过滤表信息】节点
------------------------------------------------
"""
import yaml
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime

from agent.context import DataAgentContext
from agent.llm import llm
from agent.state import DataAgentState
from core.logger import logger
from prompt.prompt_loader import load_prompt


async def filter_table(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("✅过滤表信息")

    """ 1. 用户问题 & 表信息 """
    query = state['query']
    table_infos = state['table_infos']

    """ 2. 定义 llm chain 对表信息进行过滤 """
    prompt = PromptTemplate(
        template=load_prompt(name="filter_table_info"),
        input_variables=["query", "table_infos"]
    )
    output_parser = JsonOutputParser()
    chain = prompt | llm | output_parser
    res = await chain.ainvoke(
        input={
            "query": query,
            "table_infos": yaml.dump(table_infos, allow_unicode=True, sort_keys=False) # 以yaml格式传入
        }
    )

    """ 3. 利用llm输出结果过滤 table_infos """
    """ 模型输出示例如下：
    {
        'fact_order': ['order_amount', 'region_id'],
        'dim_region': ['region_id', 'region_name']
    }
    
    下面利用 浅拷贝 对不符合要求的 表或者表内字段 进行移除操作 -> 之所以用 浅拷贝 是因为，直接对列表边遍历边删除操作会带来异常
    """
    for table_info in table_infos[:]: # table_infos[:] 表示浅拷贝 -> 列表仅仅拷贝元素的指针
        if table_info['name'] not in res: # 模型输出结果中 不存在该表 -> 移除该表
            table_infos.remove(table_info)
            continue
        for column_info in table_info['columns'][:]: # 模型输出结果中 不存在该表的某个字段 -> 移除该字段 :::: 注意同样需要进行浅拷贝
            if column_info['name'] not in res[table_info['name']]:
                table_info['columns'].remove(column_info)

    logger.info(f"过滤后的表信息为：{[ table_info.get('name') for table_info in table_infos ]}")

    return {
        "table_infos": table_infos
    }