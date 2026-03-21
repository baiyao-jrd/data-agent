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
from datetime import datetime

from langgraph.runtime import Runtime

from agent.context import DataAgentContext
from agent.state import DataAgentState, DateInfoState
from core.logger import logger


async def add_extra_context(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("✅添加额外上下文信息")

    """ 1. 添加时间信息：日期 & 星期 & 季度 """
    date = datetime.today() # 2026-03-21 09:59:25.23543253
    format_date = date.strftime("%Y-%m-%d")
    weekday = date.strftime("%A") # Saturday
    format_quarter = f"Q{(date.month - 1) // 3 + 1}"

    date_info = DateInfoState(
        date=format_date,
        weekday=weekday,
        quarter=format_quarter
    )

    """ 2. 添加数据库环境信息：方言 & 版本 """
    dw_mysql_repository = runtime.context['dw_mysql_repository']
    db_info = await dw_mysql_repository.get_db_info()

    """ 3. 存入状态 """
    logger.info(f"\n额外的上下文信息: \n - 时间信息：{date_info}\n - 数据库信息：{db_info}")
    return {
        "date_info": date_info,
        "db_info": db_info
    }