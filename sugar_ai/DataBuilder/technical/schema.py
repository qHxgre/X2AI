import numpy as np
import pandas as pd
from pydantic import Field
from base import BaseSchema


class TechnicalSchema(BaseSchema):
    """技术指标"""
    date: np.datetime64 = Field(description="日期", default=np.nan)
    instrument: pd.StringDtype = Field(description="合约代码", default=np.nan)
    high: np.double = Field(description="最高价", default=np.nan)
    open: np.double = Field(description="开盘价", default=np.nan)
    low: np.double = Field(description="最低价", default=np.nan)
    close: np.double = Field(description="收盘价", default=np.nan)
    volume: np.double = Field(description="成交量", default=np.nan)
    amount: np.double = Field(description="成交额", default=np.nan)
    bbands_upper: np.double = Field(description="布林带上轨", default=np.nan)
    bbands_middle: np.double = Field(description="布林带中轨", default=np.nan)
    bbands_lower: np.double = Field(description="布林带下轨", default=np.nan)
    rsi: np.double = Field(description="RSI", default=np.nan)
    ma_short: np.double = Field(description="短周期均线", default=np.nan)
    ma_long: np.double = Field(description="长周期均线", default=np.nan)
    kdj_k: np.double = Field(description="KDJ指标的K值", default=np.nan)
    kdj_d: np.double = Field(description="KDJ指标的D值", default=np.nan)
    kdj_j: np.double = Field(description="KDJ指标的J值", default=np.nan)
    macd: np.double = Field(description="MACD值", default=np.nan)
    macd_signal: np.double = Field(description="MACD的信号线", default=np.nan)
    macd_hist: np.double = Field(description="MACD的柱状图", default=np.nan)
    atr: np.double = Field(description="KDJ指标的J值", default=np.nan)

    class Config:
        arbitrary_types_allowed = True
