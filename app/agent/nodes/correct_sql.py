"""
------------------------------------------------
    @Time: 2026/3/16 17:05 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/correct_sql.py
    @Software: PyCharm
    @Description: 【校正SQL】节点
------------------------------------------------
"""
from langgraph.runtime import Runtime

from agent.context import DataAgentContext
from agent.state import DataAgentState

async def correct_sql(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("✅校正SQL")