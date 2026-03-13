"""
------------------------------------------------
    @Time: 2026/3/13 14:27 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/config_loader.py
    @Software: PyCharm
    @Description: 加载配置文件的通用方法
------------------------------------------------
"""
from pathlib import Path
from typing import TypeVar, Type

from omegaconf import OmegaConf

T = TypeVar("T") # 泛型

""" Schema_cls: Type[T] 类型参数 -> 将类型本身作为参数传入 """
def load_config(config_path: Path, Schema_cls: Type[T]) -> T: # Type[T] 配置类类型, T 配置类实例
    context = OmegaConf.load(config_path)
    schema = OmegaConf.structured(Schema_cls)
    config: Schema_cls = OmegaConf.to_object(OmegaConf.merge(schema, context))
    return config