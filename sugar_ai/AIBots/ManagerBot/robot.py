import os
from typing import Optional
from datetime import datetime, timedelta
from base import PARENT_PATH, AIBase

USER_PROMPT = """
舆情报告
'''
{sentimental_report}
'''

技术指标报告
'''
{techinical_report}
'''
"""

class ManagerBot(AIBase):
    """投资经理"""
    def __init__(self) -> None:
        super().__init__()
        self.filepath_prompt = PARENT_PATH + "AIBots/ManagerBot/prompts/"
        self.filepath_save = PARENT_PATH + "Reports/"

    def reporting(self) -> str:
        """根据不同研究员的报告给出最终的报告"""
        # 获取报告
        all_reports = os.listdir(PARENT_PATH+"Reports")
        reports = {
            "sentimental": "",
            "technical": "",
        }
        for report in all_reports:
            if report.split("_")[0] != datetime.now().strftime("%Y%m%d"):
                continue
            if report.split("_")[2] == "sentimental":
                # 舆情类
                if reports["sentimental"] == "":
                    reports["sentimental"] = report
                elif report.split("_")[1] > reports["technical"].split("_")[1]:
                    reports["technical"] = report
                else:
                    pass
            if report.split("_")[2] == "technical":
                # 技术指标
                if reports["technical"] == "":
                    reports["technical"] = report
                elif report.split("_")[1] > reports["technical"].split("_")[1]:
                    reports["technical"] = report
                else:
                    pass
        
        #
        sys_prompt = self.read_md(self.filepath_prompt+"manager.md")
        user_prompt = USER_PROMPT.format(
            sentimental_report = reports["sentimental"],
            techinical_report=reports["technical"],
        )

        report = self.api_deepseek(user_prompt, sys_prompt)
        # 报告保存
        self.save_md(self.filepath_save, "manager", report)
        return report

    def discussing(self) -> None:
        """与不同研究员就某份报告进行讨论"""
        pass
