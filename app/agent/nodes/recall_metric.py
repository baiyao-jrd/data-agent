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
from langgraph.runtime import Runtime

from agent.context import DataAgentContext
from agent.state import DataAgentState

async def recall_metric(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("✅指标信息召回")

