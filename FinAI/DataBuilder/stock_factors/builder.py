import numpy as np
import pandas as pd
from datetime import datetime
from DataBuilder.stock_factors.schema import StockFactorsSchema
from Base import DBFile, DBSQL, BaseBuilder, LoggerController
from bigquantdai import dai

class StockFactorsBuilder(BaseBuilder):
    datasource_id = "stock_factors"
    unique_together = ["date", "instrument"]
    sort_by = [("date", "ascending"), ("instrument", "ascending")]
    indexes = ["date"]
    schema = StockFactorsSchema

    def __init__(self, start_date: str, end_date: str, db=None) -> None:
        # 开始时间和结束时间
        self.start_date, self.end_date = start_date, end_date
        # 数据库：默认为 PostgresSQL
        self.handler = DBFile() if db is None else db

        # 日志打印
        self.logger = LoggerController(
            name="stock_factors",
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
        data = pd.read_parquet('stock_factors.parquet')
        data = data[(data['date'] >= self.start_date) & (data['date'] <= self.end_date)]
        if data.empty:
            self.logger.warning("没有符合条件的数据, 请检查数据源或日期范围")
            return
        t1 = datetime.now()
        self.logger.info(f"数据读取, 耗时: {t1 - t0}")

        normalized_df = self.normalize(data)
        normalized_df[self.handler.DEFAULT_PARTITION_FIELD] = normalized_df["date"].dt.strftime("%Y%m")
        self.write(normalized_df)
        t2 = datetime.now()
        self.logger.info(f"数据存储完毕, 耗时: {t2 - t1}")