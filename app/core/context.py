"""
------------------------------------------------
    @Time: 2026/3/12 20:39 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/context.py
    @Software: PyCharm
    @Description: 日志上下文
        每条日志包含一个与请求一一对应的request_id
        这样能保证异步环境下的任务日志可以被明确区分
------------------------------------------------
"""
from contextvars import ContextVar

request_id_ctx_var = ContextVar("request_id")

