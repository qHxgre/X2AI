import numpy as np
import pandas as pd
from pydantic import Field
from Base.DataBase import BaseSchema


class AIBotSentimentalSugarResearcherSchema(BaseSchema):
    """AI机器人-舆情分析-白糖研究员"""

    date: np.datetime64 = Field(description="发布日期", default=np.nan)
    title: pd.StringDtype = Field(description="文章标题", default='')
    category: pd.StringDtype = Field(description="分类", default='')
    sub_category: pd.StringDtype = Field(description="子分类", default='')
    summary: pd.StringDtype = Field(description="内容总结", default='')
    opinion: pd.StringDtype = Field(description="分析观点", default='')
    short_forecast: np.double = Field(description="短期预测", default=np.nan)
    long_forecast: np.double = Field(description="长期预测", default=np.nan)
    confidence: np.double = Field(description="置信度", default=np.nan)

    class Config:
        arbitrary_types_allowed = True
