import numpy as np
import pandas as pd
from datetime import datetime
from DataBuilder.future_bar1d.schema import FutureBar1dSchema
from Base import DBFile, DBSQL, BaseBuilder, LoggerController
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

        # 日志打印
        self.logger = LoggerController(
            name="future_bar1d",
            log_level="INFO",
            console_output=False,
            file_output=True,
            log_file="Builder.log",
            when='D'
        )

        # 初始化的日志
        self.logger.info(f"本次数据构建的创建时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"表名: {self.datasource_id}, 构建周期: {self.start_date} 至 {self.end_date}")


    def build(self):
        t0 = datetime.now()
        access_key = "qENDSBnmh51D"
        secret_key = "BwaziOkvccKfYg5txdcXgX8xo2yzcmLReeoePN1t3VA1UYabYi6mGQb7MtePabF0"
        dai.login(access_key, secret_key)

        data = dai.DataSource("cn_future_bar1d").read_bdb(
            as_type=pd.DataFrame,
            partition_filter={
                "date": (self.start_date, self.end_date),
                "instrument": {"SR8888.CZC"}
            })
        t1 = datetime.now()
        self.logger.info(f"数据读取完成，大小: {data.shape}, 耗时: {t1 - t0}")

        normalized_df = self.normalize(data)
        normalized_df[self.handler.DEFAULT_PARTITION_FIELD] = normalized_df["date"].dt.strftime("%Y%m")
        self.write(normalized_df)
        t2 = datetime.now()
        self.logger.info(f"数据存储完毕, 耗时: {t2 - t1}")