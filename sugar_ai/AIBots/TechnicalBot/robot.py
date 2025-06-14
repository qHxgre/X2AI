import dai
import numpy as np
import pandas as pd
from typing import Optional
from datetime import datetime, timedelta
from X2AI.sugar_ai.Base.base import PARENT_PATH, AIBase


class TechnicalBot(AIBase):
    """基本面AI研究员
    1. 输入单篇文章进行总结和分析
    2. 将第1步的结论进行最后总结
    """
    def __init__(self, today: Optional[str]=None, n_days: int=60) -> None:
        super().__init__()
        date_format = "%Y-%m-%d"
        self.end_date = today if today is not None else datetime.now().strftime(date_format)
        self.start_date = (datetime.strptime(self.end_date, date_format) - timedelta(days=n_days)).strftime(date_format)

        self.filepath_prompt = PARENT_PATH + "AIBots/TechnicalBot/prompts/"
        # self.filepath_cache = PARENT_PATH + "AIBots/TechnicalBot/cache/"
        self.filepath_save = PARENT_PATH + "Reports/"

    def researcher(self) -> str:
        """研究员"""
        sys_prompt = self.read_md(self.filepath_prompt+"researcher.md")
        data = self.get_data("aisugar_technical", self.start_date, self.end_date).sort_values('date')
        data["date"] = data["date"].dt.strftime("%Y-%m-%d")
        user_prompt = data.set_index("date").to_markdown()

        report = self.api_deepseek(user_prompt, sys_prompt)

        # 报告保存
        self.save_md(self.filepath_save, "technical", report)
        return report

    def analyzing(self) -> None:
        report = self.researcher()
        self.email_sending(f"SR 技术分析报告_{self.end_date}", report)
