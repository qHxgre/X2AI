import os
import re
import pandas as pd
from datetime import datetime, timedelta
from flask import jsonify
from Base import LoggerController
from AIBots.StockBot.robot import StockBot
from Base import DBFile
from AIBots.SentimentalBot.templets import TEMPLET_REPORT

class stockService:
    def __init__(self, bot: StockBot, logger: LoggerController):
        self.bot = bot
        self.logger = logger
        self.logger.info("stockService initialized")
    
    def get_results(self):
        results = DBFile().read_dict(
            table="stock_recommend",
        )
        # 获取最新日期的数据（假设results是按日期组织的字典）
        latest_date = max(results.keys())
        result = results[latest_date]
        sorted_results = dict(sorted(result.items(), key=lambda item: item[1]['rank']))
        self.logger.info(f"Returning stock results for date: {latest_date}")
        return sorted_results