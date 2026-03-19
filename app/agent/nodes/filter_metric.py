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
from langgraph.runtime import Runtime

from agent.context import DataAgentContext
from agent.state import DataAgentState

async def filter_metric(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("✅过滤指标信息")