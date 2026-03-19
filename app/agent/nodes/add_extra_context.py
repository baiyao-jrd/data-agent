"""
------------------------------------------------
    @Time: 2026/3/16 17:01 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/add_extra_context.py
    @Software: PyCharm
    @Description: 【添加额外上下文信息】节点
------------------------------------------------
"""
from langgraph.runtime import Runtime

from agent.context import DataAgentContext
from agent.state import DataAgentState

async def add_extra_context(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("✅添加额外上下文信息")