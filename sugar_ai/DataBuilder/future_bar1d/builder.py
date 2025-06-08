import time
import schedule
import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from DataBuilder.future_bar1d.schema import FutureBar1dSchema
from base import DBFile, DBSQL, BaseBuilder
from bigquantdai import dai

class FutureBar1dBuilder(BaseBuilder):
    datasource_id = "future_bar1d"
    unique_together = ["date", "instrument"]
    sort_by = [("date", "ascending"), ("instrument", "ascending")]
    indexes = ["date"]
    schema = FutureBar1dSchema

    def __init__(self, start_date: str, end_date: str, db=None) -> None:
        # 开始时间和结束时间
        self.start_date, self.end_date = start_date, end_date
        # 数据库：默认为 PostgresSQL
        self.handler = DBSQL() if db is None else db
        # 原始数据
        self.raw_data = self.handler.read_data(
            table=self.datasource_id,
            filters={
                "date": [self.start_date, self.end_date]
            }
        )


    def build(self):
        access_key = "qENDSBnmh51D"
        secret_key = "BwaziOkvccKfYg5txdcXgX8xo2yzcmLReeoePN1t3VA1UYabYi6mGQb7MtePabF0"
        dai.login(access_key, secret_key)

        data = dai.DataSource("cn_future_bar1d").read_bdb(
            as_type=pd.DataFrame,
            partition_filter={
                "date": (self.start_date, self.end_date),
                "instrument": {"SR8888.CZC"}
            })
    
        normalized_df = self.normalize(data)
        normalized_df[self.handler.DEFAULT_PARTITION_FIELD] = normalized_df["date"].dt.strftime("%Y%m")
        self.write(normalized_df)