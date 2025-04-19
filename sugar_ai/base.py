
ENABLE_BQ = False

if ENABLE_BQ:
    import dai
    from warehouse.crawler.proxies import proxypool

import os
import uuid
import pickle
import numpy as np
import pandas as pd
import pydantic
import requests
from requests import Response
from datetime import datetime, timedelta
from typing import Any, Dict, List, Union, Optional
from DataBase.transfer import DataTransfer

from openai import OpenAI

import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart



PARENT_PATH = os.path.abspath(__file__).replace("base.py", "")

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
        if ENABLE_BQ:
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


class BaseCrawler(BaseBuilder):
    """爬虫基础类"""
    def __init__(self) -> None:
        pass

    def get_proxies(self) -> Dict[str, str]:
        """随机获取两个代理"""
        if ENABLE_BQ:
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


class AIBase:
    def __init__(self) -> None:
        """初始化"""
        self.today = datetime.now().strftime("%Y%m%d")
        self.time = datetime.now().strftime("%H%M%S")

    def get_data(self, table: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取数据"""
        if ENABLE_BQ:
            return dai.query(f"SELECT * FROM {table}", filters={"date": [start_date, end_date]}).df().sort_values("date", ascending=False)
        else:
            data = DataTransfer(f"/Users/xiehao/Desktop/workspace/X2AI/sugar_ai/DataBase/{table}").load(
                start_date=start_date,
                end_date=end_date,
            ).sort_values("date", ascending=False)
            return data

    def api_deepseek(self, user_prompt: str, sys_prompt: str, json_output: bool=False) -> str:
        client = OpenAI(api_key="sk-7e0d7d183ae84e08b8579a537feff921", base_url="https://api.deepseek.com")

        if json_output is True:
            response = client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": sys_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                stream=False,
                response_format={'type': 'json_object'}
            )
        else:
            response = client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": sys_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                stream=False
            )

        answer = response.choices[0].message.content
        return answer

    def read_md(self, filepath: str) -> str:
        """读取指定 Markdown 文件"""
        with open(filepath, 'r', encoding='utf-8') as file:
            content = file.read()
        return content

    def save_md(self, filepath: str, category: str, content: str) -> str:
        """保存内容到指定 Markdown 文件"""
        report_id = "{today}_{category}_{report_id}.md".format(
            today=self.today,
            category=category,
            report_id=str(uuid.uuid4())
        )
        with open(filepath+report_id, 'w', encoding='utf-8') as file:
            file.write(content)

    def cache(self, filepath: str, content: Optional[Union[str, dict]]=None):
        """
        加载缓存或生成数据并保存
        :param content: 缓存内容
        :param filepath: 缓存文件名
        """
        # 加载缓存
        if os.path.exists(filepath):
            try:
                with open(filepath, 'rb') as f:
                    return pickle.load(f)
            except (FileNotFoundError, pickle.UnpicklingError, EOFError) as e:
                print(f"缓存加载失败，重新生成: {e}")
        
        # 保存缓存
        if content is not None:
            try:
                with open(filepath, 'wb') as f:
                    pickle.dump(content, f)
            except IOError as e:
                print(f"缓存保存失败: {e}")

    def email_sending(self, title: str, content: str) -> None:
        """发送邮件"""
        sender_email = "253950805@qq.com"
        sender_password = "xhpwvoopregscagj"
        receiver_email = "253950805@qq.com"

        # 构建邮件内容
        msg = MIMEMultipart("alternative")
        msg["Subject"] = Header(title, "utf-8")
        msg["From"] = sender_email
        msg["To"] = receiver_email
        html_message = MIMEText(content, "plain", "utf-8")
        html_message["Accept-Language"] = "zh-CN"
        html_message["Aceept-CHarset"] = "ISO-8859-1, utf-8"
        msg.attach(html_message)

        try:
            server = smtplib.SMTP_SSL('smtp.qq.com', 465, timeout=10)
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
            print(f"{self.today} 研究报告, 邮件发送成功!")
        except Exception as e:
            print(f"{self.today} 研究报告, 邮件发送失败: ", e)
        finally:
            server.quit()
