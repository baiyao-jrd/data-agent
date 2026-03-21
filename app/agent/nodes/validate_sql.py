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
from core.logger import logger


async def validate_sql(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("✅校验SQL")

    """ 1. 获取 生成的sql """
    sql = state['sql']

    """ 2. 校验 """
    dw_mysql_repository = runtime.context['dw_mysql_repository']

    try:
        await dw_mysql_repository.validate_sql(statement=sql)
        logger.info(f"✅SQL校验成功")
        return {"error": None}
    except Exception as e:
        logger.error(f"❌SQL校验失败: {e}")
        return {"error": str(e)}