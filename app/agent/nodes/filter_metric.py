"""
------------------------------------------------
    @Time: 2026/3/16 16:59 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/filter_metric.py
    @Software: PyCharm
    @Description: 【过滤指标信息】节点
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


async def filter_metric(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("✅过滤指标信息")

    """ 1. 用户问题 & 指标信息 """
    query = state['query']
    metric_infos = state['metric_infos']

    """ 2. 定义 llm chain 对指标信息进行过滤 """
    prompt = PromptTemplate(
        template=load_prompt(name="filter_metric_info"),
        input_variables=["query", "metric_infos"]
    )
    output_parser = JsonOutputParser()
    chain = prompt | llm | output_parser
    res = await chain.ainvoke(
        input={
            "query": query,
            "metric_infos": yaml.dump(metric_infos, allow_unicode=True, sort_keys=False)  # 以yaml格式传入
        }
    )

    logger.info(f"yaml.dump(metric_infos) ::::: \n{yaml.dump(metric_infos, allow_unicode=True, sort_keys=False)}") # 使得中文字符集能够正常显示

    """ 3. 利用llm输出结果过滤 metric_infos """
    """ 模型输出示例如下：
    [
      "实习生人数",
      "转正率"
    ]
    """
    for metric_info in metric_infos[:]:
        if metric_info['name'] not in res:
            metric_infos.remove(metric_info)

    logger.info(f"过滤后的指标信息为：{[metric_info.get('name') for metric_info in metric_infos]}")

    return {
        "metric_infos": metric_infos
    }