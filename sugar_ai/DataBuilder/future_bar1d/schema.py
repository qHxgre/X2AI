import numpy as np
import pandas as pd
from pydantic import Field
from Base import BaseSchema


class FutureBar1dSchema(BaseSchema):
    """期货日行情 Schema"""

    date: np.datetime64 = Field(description="日期", default=np.nan)
    instrument: pd.StringDtype = Field(description="合约代码", default=np.nan)
    trading_code: pd.StringDtype = Field(description="交易代码", default=np.nan)
    open: np.double = Field(description="开盘价", default=np.nan)
    close: np.double = Field(description="收盘价", default=np.nan)
    high: np.double = Field(description="最高价", default=np.nan)
    low: np.double = Field(description="最低价", default=np.nan)
    pre_close: np.double = Field(description="前收盘价", default=np.nan)
    volume: np.int64 = Field(description="成交量", default=0)
    amount: np.double = Field(description="成交金额", default=np.nan)
    open_interest: np.int32 = Field(description="持仓量", default=0)
    settle: np.double = Field(description="结算价", default=np.nan)
    pre_settle: np.double = Field(description="前结算价", default=np.nan)
    upper_limit: np.double = Field(description="涨停价", default=np.nan)
    lower_limit: np.double = Field(description="跌停价", default=np.nan)
    product_code: pd.StringDtype = Field(description="品种代码", default=np.nan)

    class Config:
        arbitrary_types_allowed = True
