"""
------------------------------------------------
    @Time: 2026/3/16 17:04 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/validate_sql.py
    @Software: PyCharm
    @Description: 【校验SQL】节点
------------------------------------------------
"""
from langgraph.runtime import Runtime

from agent.context import DataAgentContext
from agent.state import DataAgentState

async def validate_sql(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("✅校验SQL")

    return {"error": None}