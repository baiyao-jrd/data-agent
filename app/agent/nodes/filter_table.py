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
from langgraph.runtime import Runtime

from agent.context import DataAgentContext
from agent.state import DataAgentState

async def filter_table(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("✅过滤表信息")