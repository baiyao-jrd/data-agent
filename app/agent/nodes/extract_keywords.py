"""
------------------------------------------------
    @Time: 2026/3/16 16:14 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/extract_keywords.py
    @Software: PyCharm
    @Description: 【提取关键字】节点
------------------------------------------------
"""
import jieba.analyse
from langgraph.runtime import Runtime

from agent.context import DataAgentContext
from agent.state import DataAgentState
from core.logger import logger


async def extract_keywords(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("✅提取关键字")

    """ 1. 提取用户查询 """
    query = state["query"]

    """ 2. 按照所需词性进行关键字抽取 """
    allow_pos = (
        "n", # 名词：数据、服务器、表格
        "rr", # 人名：张绍刚、李政道
        "ns", # 地名：北京、上海、广州
        "nt", # 机构名：百度、阿里、腾讯
        "nz", # 专有名词：哈希算法、诺贝尔奖
        "v", # 动词：查询、更新、删除
        "vn", # 名动词：工作、研究
        "a", # 形容词：快速、大型
        "an", # 名形容词：复杂度、合法性
        "eng", # 英文
        "i", # 成语
        "l" # 常用固定短语
    )

    keywords = jieba.analyse.extract_tags(
        sentence=query,
        topK=20,
        allowPOS=allow_pos
    )

    """ 3. 为防止信息丢失，将原查询存放至关键字列表中 """
    keywords = list(set(keywords + [query]))

    logger.info(f"✋🏻查询的关键字为：{keywords}")

    """ 4. 将抽取出的关键字存放至状态当中 """
    return {"keywords": keywords}