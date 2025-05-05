import dai
import pandas as pd
import requests
from requests import Response
from typing import Dict
from warehouse.crawler.proxies import proxypool

from DataBuilder.hisugar.crawler import HigSugarCrawler
from base_bq import BaseBuilder
from DataBuilder.hisugar.schema import HisugarSchema

class HigSugarCrawlerBQ(HigSugarCrawler, BaseBuilder):
    """BQ 上进行爬虫"""
    def __init__(self) -> None:
        super().__init__()
        self.RETRIES = 5
    
        self.datasource_id = "aisugar_hisugar"
        self.unique_together = ["date", "article_id", "category", "sub_category", "title"]
        self.sort_by = [("date", "ascending"), ("article_id", "ascending")]
        self.indexes = ["date"]
        self.schema = HisugarSchema

    def get_old_data(self, table: str, category_name: str, sub_name: str, sd: str, ed: str) -> pd.DataFrame:
        """获取已有数据"""
        data = dai.query(f"""
        SELECT *
        FROM {table}
        WHERE category = '{category_name}'
        AND sub_category = '{sub_name}'
        """, filters={'date': [sd, ed]}).df()
        return data

    def get_proxies(self) -> Dict[str, str]:
        """随机获取两个代理"""
        return proxypool.random()

    def request(self, url, params=None, headers=None,) -> Response:
        tried = 0       # 已尝试次数
        exception = None
        proxies = self.get_proxies()
        while tried < self.RETRIES:
            try:
                return requests.get(url, params=params, headers=headers, proxies=proxies)
            except Exception as e:
                exception = e
                tried = tried + 1
                proxies = self.get_proxies()
        if exception:
            raise exception
        return Response()

    def save_data(self, df: pd.DataFrame) -> None:
        """存储数据"""
        normalized_df = self.normalize(df)
        self.dai_write(normalized_df)