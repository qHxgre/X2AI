import os
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Dict
from joblib import Parallel, delayed
from Base import BaseAI, DBFile, DBSQL, LoggerController
from AIBots.SentimentalBot.templets import TEMPLET_ARTICLE, TEMPLET_ASSISTANT_REPORT, TEMPLET_REPORT

from pyecharts.charts import Kline
from pyecharts import options as opts

TEMPLET_DATA = """
## 第 {index} 个股票数据
* 股票代码: {instrument}
* pe_ttm: {pe_ttm}
* roe_ttm: {roe_ttm}
* net_profit_ttm_yoy: {net_profit_ttm_yoy}
* net_profit_to_parent_ttm_yoy: {net_profit_to_parent_ttm_yoy}
"""

TEMPLET_FACTOR = """
## {value}
* 中文名称: {name}
* 投资用法: {describe}
"""

class StockBot(BaseAI):
    def __init__(
        self,
        start_date: Optional[str]=None,
        end_date: Optional[str]=None,
        n_days: int=7,
        db: Optional[DBFile]=None
    ) -> None: 
        super().__init__()
        
        # 日期范围
        date_format = "%Y-%m-%d"
        self.start_date = (datetime.now() - timedelta(days=n_days)).strftime(date_format) if start_date is None else start_date
        self.end_date = datetime.now().strftime(date_format) if end_date is None else end_date

        # 数据库
        self.handler = DBFile(os.path.join(self.parent_path, "DataBase")) if db is None else db

        # 文件路径
        self.filepath_prompt = os.path.join(self.parent_path, "AIBots", "StockBot", "prompts")


        # 日志打印
        self.logger = LoggerController(
            name="SentimentalBot",
            log_level="INFO",
            console_output=False,
            file_output=True,
            log_file="AIBot_sentimental.log",
            when='D'
        )

    def get_data(self, start_date, end_date) -> pd.DataFrame:
        """
        获取数据"""
        data = DBFile().read_data(
            table="stock_factors",
            filters={"date":[start_date, end_date]},
        )

        df = data[data["date"]==data["date"].max()]
        df.loc[:, ['pe_ttm', 'roe_ttm']] = df.loc[:, ['pe_ttm', 'roe_ttm']].round(3)
        df.loc[:, ['net_profit_ttm_yoy', 'net_profit_to_parent_ttm_yoy']] = df.loc[:, ['net_profit_ttm_yoy', 'net_profit_to_parent_ttm_yoy']].round(4)
        # 暂时只选择100个股票进行分析
        import random
        df = df[df["instrument"].isin(random.sample(df["instrument"].unique().tolist(), 100))]
        result = df.set_index("instrument").drop(columns=["date"]).to_dict(orient="index")
        return result

    
    def analyzing(self):
        data = self.get_data(self.start_date, self.end_date)

        # 构建用户提示词
        user_prompt = ""
        for i, (instrument, factors) in enumerate(data.items()):
            data_str = TEMPLET_DATA.format(
                index=i + 1,
                instrument=instrument,
                pe_ttm=factors.get("pe_ttm", "N/A"),
                roe_ttm=factors.get("roe_ttm", "N/A"),
                net_profit_ttm_yoy=factors.get("net_profit_ttm_yoy", "N/A"),
                net_profit_to_parent_ttm_yoy=factors.get("net_profit_to_parent_ttm_yoy", "N/A"),
            )
            user_prompt += data_str

        # 构建系统提示词
        with open("mapping.json", "r") as f:
            mapping = json.load(f)
        indicators_introducation = ""
        for v in mapping:
            field_str = TEMPLET_FACTOR.format(
                value=v["field"],
                name=v["name"],
                describe=v["describe"]
            )
            indicators_introducation += field_str
        SYS_PROMPT = self.read_md(os.path.join(self.filepath_prompt, "stock_researcher.md"))
        system_prompt = SYS_PROMPT.format(indicators_introducation=indicators_introducation)

        # AI 分析
        answer = self.api_deepseek(user_prompt, system_prompt, True)
        recommend = json.loads(answer)

        result = {}
        for i in recommend["recommendations"]:
            result[i["instrument"]] = {
                "pe_ttm": data[i["instrument"]]["pe_ttm"],
                "roe_ttm": data[i["instrument"]]["roe_ttm"],
                "net_profit_ttm_yoy": data[i["instrument"]]["net_profit_ttm_yoy"],
                "net_profit_to_parent_ttm_yoy": data[i["instrument"]]["net_profit_to_parent_ttm_yoy"],
                "rank": i["rank"],
                "reason": i["reason"]
            }
        
        self.handler.save_dict(
            data={self.end_date: result},
            table="stock_recommend",
        )