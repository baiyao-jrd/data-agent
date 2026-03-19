"""
------------------------------------------------
    @Time: 2026/3/18 15:10 
    @Author: RunDong
    @Email: 18303620306@163.com
    @File: data-agent/prompt_loader.py
    @Software: PyCharm
    @Description: 读取提示词的通用工具
------------------------------------------------
"""
from pathlib import Path

def load_prompt(name: str) -> str:
    file = Path(__file__).parents[2] / "prompts" / f"{name}.prompt"
    return file.read_text(encoding="utf-8")

if __name__ == '__main__':
    print(load_prompt("extend_keywords_for_column_recall"))