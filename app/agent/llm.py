"""
------------------------------------------------
    @Time: 2026/3/18 14:36 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/llm.py
    @Software: PyCharm
    @Description: 智能体 - 全局模型 - deepseek
------------------------------------------------
"""
from langchain.chat_models import init_chat_model

from conf.app_config import app_config
from core.logger import logger

llm = init_chat_model(
    model = app_config.llm.model_name,
    api_key = app_config.llm.api_key,
    temperature = 0, # 使回答尽可能确定而不是随机
)

# resp = llm.invoke(input="介绍一下北京？")
#
# logger.info(f"LLM: {resp.content}")