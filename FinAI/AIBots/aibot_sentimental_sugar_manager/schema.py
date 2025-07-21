import numpy as np
import pandas as pd
from pydantic import Field
from Base.DataBase import BaseSchema


class AIBotSentimentalSugarManagerSchema(BaseSchema):
    """AI机器人-舆情分析-白糖基金经理"""

    date: np.datetime64 = Field(description="发布日期", default=np.nan)
    update_time: np.datetime64 = Field(description="更新时间", default=np.nan)
    llms: pd.StringDtype = Field(description="AI模型", default='')
    method: pd.StringDtype = Field(description="分析方式", default='')
    rating: pd.StringDtype = Field(description="投资评级", default='')
    ranking: np.double = Field(description="预测值", default=np.nan)
    confidence: np.double = Field(description="置信度", default=np.nan)
    conclusion: pd.StringDtype = Field(description="分析总结", default='')
    bullish_supply: pd.StringDtype = Field(description="看多-供应", default='')
    bullish_demand: pd.StringDtype = Field(description="看多-需求", default='')
    bullish_energy: pd.StringDtype = Field(description="看多-能源原油", default='')
    bullish_domestic: pd.StringDtype = Field(description="看多-国内因素", default='')
    bullish_market: pd.StringDtype = Field(description="看多-市场情绪", default='')
    bearish_supply: pd.StringDtype = Field(description="看空-供应", default='')
    bearish_demand: pd.StringDtype = Field(description="看空-需求", default='')
    bearish_energy: pd.StringDtype = Field(description="看空-能源原油", default='')
    bearish_domestic: pd.StringDtype = Field(description="看空-国内因素", default='')
    bearish_market: pd.StringDtype = Field(description="看空-市场情绪", default='')

    class Config:
        arbitrary_types_allowed = True

