import numpy as np
import pandas as pd
from pydantic import Field
from Base import BaseSchema


class StockFactorsSchema(BaseSchema):
    """股票因子"""
    date: np.datetime64 = Field(description="日期", default=np.nan)
    instrument: pd.StringDtype = Field(description="证券代码", default=np.nan)
    pe_ttm: np.float64 = Field(description="市盈率TTM", default=np.nan)
    roe_ttm: np.float64 = Field(description="净资产收益率(TTM)", default=np.nan)
    net_profit_ttm_yoy: np.float64 = Field(description="净利润增长率(TTM)", default=np.nan)
    net_profit_to_parent_ttm_yoy: np.float64 = Field(description="归母净利润增长率(TTM)", default=np.nan)

    class Config:
        arbitrary_types_allowed = True
