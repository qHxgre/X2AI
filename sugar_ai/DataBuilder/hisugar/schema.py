import numpy as np
import pandas as pd
from pydantic import Field
from base import BaseSchema


class HisugarSchema(BaseSchema):
    """Hisugar"""
    article_id: pd.StringDtype = Field(description="文章ID", default='')
    date: np.datetime64 = Field(description="发布日期", default=np.nan)
    category: pd.StringDtype = Field(description="分类", default='')
    sub_category: pd.StringDtype = Field(description="子类", default='')
    title: pd.StringDtype = Field(description="标题", default='')
    brief: pd.StringDtype = Field(description="简介", default='')
    content: pd.StringDtype = Field(description="内容", default='')

    class Config:
        arbitrary_types_allowed = True
