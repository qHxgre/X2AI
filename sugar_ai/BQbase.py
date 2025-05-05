import dai
import numpy as np
import pandas as pd
import pydantic
import requests
from typing import Any, Dict, List
from requests import Response
from warehouse.crawler.proxies import proxypool


class BaseBuilder:
    """数据构建类"""
    def __init__(self) -> None:
        pass

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.reindex(columns=self.schema.columns())
        df = df.astype(self.schema.field_type_mapping())
        df = df.fillna(self.schema.field_default_mapping())
        return df

    def dai_write(self, df: pd.DataFrame) -> None:
        default_docs = self.schema.default_docs()
        df[dai.DEFAULT_PARTITION_FIELD] = df["date"].dt.year.astype("int64")
        dai.DataSource.write_bdb(
            df,
            id=self.datasource_id,
            unique_together=self.unique_together,
            sort_by=self.sort_by,
            indexes=self.indexes,
            docs=default_docs,
        )


class BaseSchema(pydantic.BaseModel):
    """数据描述"""
    @classmethod
    def field_type_mapping(cls) -> Dict[str, Any]:
        """字段和类型的映射: df.astype(field_type_mapping)
        """
        fields = cls.model_fields  # type: ignore
        schema = {}
        for field, fieldinfo in fields.items():
            value = fieldinfo.annotation
            if hasattr(pd, fieldinfo.annotation.__name__):  # type: ignore
                value = fieldinfo.annotation()  # type: ignore
            if value is np.datetime64:
                value = "datetime64[ns]"
            schema[field] = value
        return schema

    @classmethod
    def columns(cls) -> List[str]:
        """所有字段列表"""
        return list(cls.model_fields.keys())  # type: ignore

    @classmethod
    def field_default_mapping(cls) -> Dict[str, Any]:
        """字段和默认值的映射

        使用场景: df.fillna(field_default_mapping)
        """
        fields = cls.model_fields  # type: ignore
        return {field: fieldinfo.get_default() for field, fieldinfo in fields.items()}

    @classmethod
    def default_docs(cls) -> Dict[str, Any]:
        """表 Schema 信息
        """
        fields = cls.model_fields  # type: ignore
        return {
            "schema": {
                field: {"description": fieldinfo.description}
                for rank, (field, fieldinfo) in enumerate(fields.items())
            }
        }


class BaseCrawler:
    """爬虫基础类"""
    def __init__(self) -> None:
        pass

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
