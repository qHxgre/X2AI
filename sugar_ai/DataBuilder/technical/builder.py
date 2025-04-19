import dai
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
from base import BaseBuilder
from DataBuilder.technical.schema import TechnicalSchema

class SugarTechnicalBuilder(BaseBuilder):
    datasource_id = "aisugar_technical"
    unique_together = ["date", "instrument",]
    sort_by = [("date", "ascending"), ("instrument", "ascending")]
    indexes = ["date"]
    schema = TechnicalSchema

    def __init__(self, today: Optional[str]=None) -> None:
        date_format = "%Y-%m-%d"
        self.end_date = today if today is not None else datetime.now().strftime(date_format)
        self.before_date = (datetime.strptime(self.end_date, date_format) - timedelta(days=360)).strftime(date_format)
        self.start_date = (datetime.strptime(self.end_date, date_format) - timedelta(days=30)).strftime(date_format)
        print(f"初始化 {self.end_date}, 数据获取周期: {self.before_date} 至 {self.end_date}, 数据存储周期: {self.start_date} 至 {self.end_date}")
        self.data = pd.DataFrame()

    def save_data(self, df: pd.DataFrame) -> None:
        """存储数据"""
        if df.shape[0] == 0:
            print("当日无数据, 跳过数据存储")
            return
        normalized_df = self.normalize(df)
        self.dai_write(normalized_df)

    def get_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """获取数据"""
        SQL_TEMPLET = """
        SELECT 
            date, instrument, high, open, low, close, volume, amount,

            -- 布林带
            m_ta_bbands(close, timeperiod:=5, nbdevup:=2, nbdevdn:=2) as _bollinger_bands,
            _bollinger_bands[1] as bbands_upper,
            _bollinger_bands[2] as bbands_middle,
            _bollinger_bands[3] as bbands_lower,

            -- RSI
            m_ta_rsi(close, 5) as rsi,

            -- 均线
            m_avg(close, 5) as ma_short,
            m_avg(close, 10) as ma_long,

            -- KDJ 指标
            m_ta_kdj(high, low, close, fastk_period:=9, slowk_period:=3, slowd_period:=3, slowk_matype:=1, slowd_matype:=1) as _kdj,
            _kdj[1] as kdj_k,
            _kdj[2] as kdj_d,
            _kdj[3] as kdj_j,

            -- MACD 指标
            m_ta_macd(close, fastperiod:=12, slowperiod:=26, signalperiod:=9) as _macd,
            _macd[1] as macd,
            _macd[2] as macd_signal,
            _macd[3] as macd_hist,

            -- ATR 指标
            m_ta_atr(high, low, close, 5) as atr,
        FROM cn_future_bar1d
        WHERE instrument='SR8888.CZC'
        """
        data = dai.query(SQL_TEMPLET, filters={'date': [start_date, end_date]}).df().sort_values(["date", "instrument"])
        data["date"] = data["date"].dt.strftime("%Y-%m-%d")
        return data

    def ma(self, df: pd.DataFrame) -> pd.DataFrame:
        """移动平均指标"""
        # df = data[["date", "instrument", "ma_short", "ma_long"]].dropna()
        # df["ma_signal"] = 0
        # df.loc[
        #     (
        #         (df["ma_short"].shift(1) < df["ma_long"].shift(1))
        #         & (df["ma_short"] > df["ma_long"])
        #     ), "ma_signal"
        # ] = 1
        # return df[["date", "instrument", "ma_signal"]]
        return df

    def bollinger_bands(self) -> None:
        """布林带"""
        pass

    def rsi(self) -> None:
        """RSI指标"""
        pass

    def kdj(self) -> None:
        """KDJ指标"""
        pass

    def MACD(self) -> None:
        """MACD指标"""
        pass

    def ATR(self) -> None:
        """ATR指标"""
        pass

    def run(self) -> pd.DataFrame:
        t0 = datetime.now()
        self.data = self.get_data(self.before_date, self.end_date)
        t1 = datetime.now()
        print(f"获取原始数据: {self.before_date}, {self.end_date}, 耗时: {t1-t0}")

        self.data = self.data[(self.data["date"]>=self.start_date) & (self.data["date"]<=self.end_date)]
        self.save_data(self.data)
        t2 = datetime.now()
        print(f"存储数据, {self.end_date}, 耗时: {t2-t1}")
