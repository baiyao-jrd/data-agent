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
from agent.state import DataAgentState

async def merge_retrieved_info(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("✅合并召回信息")