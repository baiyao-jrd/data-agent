"""
------------------------------------------------
    @Time: 2026/3/14 16:52 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/embedding_client_manager.py
    @Software: PyCharm
    @Description:
------------------------------------------------
"""
from typing import Optional
from langchain_huggingface import HuggingFaceEndpointEmbeddings

from conf.app_config import EmbeddingConfig, app_config


class EmbeddingClientManager:
    def __init__(self, embedding_config: EmbeddingConfig):
        self.client: Optional[HuggingFaceEndpointEmbeddings] = None
        self.config = embedding_config

    def get_url(self):
        url = f"http://{self.config.host}:{self.config.port}"
        return url

    def init(self):
        self.client = HuggingFaceEndpointEmbeddings(model=self.get_url())

embedding_client_manager = EmbeddingClientManager(app_config.embedding)