
# 基础包
import os
import pandas as pd
from functools import reduce
from typing import Dict, Union, Optional

# 邮件发送
import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from Base.DataBase import DBFile
from Base.LoggingBase import LoggerController

# AI
from openai import OpenAI

class BaseAI:
    def __init__(self) -> None:
        """初始化"""
        self.parent_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        # 默认为文件数据库
        self.handler = DBFile()

        # 默认日志
        self.logger = LoggerController(
            name="AIBase",
            log_level="INFO",
            console_output=False,
            file_output=True,
            log_file="AIBase.log",
            when='D'
        )

    def get_data(self, table: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取数据"""
        return self.handler.read_data(
            table=table,
            filters={
                "date": [start_date, end_date]
            }
        )

    def api_deepseek(self, user_prompt: str, sys_prompt: str, json_output: bool=False) -> str:
        client = OpenAI(api_key="sk-7e0d7d183ae84e08b8579a537feff921", base_url="https://api.deepseek.com")

        try:
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
        except Exception as e:
            self.logger.warning(f"AI 分析失败: {e}")
            return None

        answer = response.choices[0].message.content
        return answer

    def read_md(self, filepath: str) -> str:
        """读取指定 Markdown 文件"""
        with open(filepath, 'r', encoding='utf-8') as file:
            content = file.read()
        return content

    def save_md(self, filepath: str, category: str, report_date: str, content: str) -> str:
        """保存内容到指定 Markdown 文件"""
        report_id = "{report_date}_{category}.md".format(
            report_date=report_date,        # 报告日期
            category=category
        )
        with open(os.path.join(filepath, report_id), 'w', encoding='utf-8') as file:
            file.write(content)

    def read_cache(self, data: Union[pd.DataFrame, Dict], input_filters: dict) -> Optional[dict]:
        """读取命中缓存
        # input_filters: 输入的过滤器，用于筛选data中是否有符合的数据
        比如: 
            input_filters = {
                "date": '2025-06-15',',
                "category": '国内新闻',
                "subcategory": ‘最新资讯,
                "title": '文章标题',
            }
        """
        if isinstance(data, pd.DataFrame):
            # 根据
            mask = pd.Series(True, index=data.index)
            for k, v in input_filters.items():
                mask &= (data[k] == v)
            
            content = data[mask]
            if content.shape[0] != 0:
                self.logger.debug(f"命中缓存: {input_filters}")
                return content.iloc[0].to_dict()
            else:
                self.logger.debug(f"未命中缓存: {input_filters}")
                return None
        elif isinstance(data, Dict):
            for k, v in data.items():
                if k == pd.to_datetime(input_filters["date"]):
                    self.logger.debug(f"命中缓存: {input_filters}")
                    return v
            self.logger.debug(f"未命中缓存: {input_filters}")
            return None
        else:
            self.logger.warning("数据格式不正确，无法读取缓存")
            return None

    def save_cache(self, data: Union[pd.DataFrame, Dict], table: str, keys: Optional[list]=None) -> None:
        """保存缓存"""
        if isinstance(data, pd.DataFrame):  
            data["date"] = pd.to_datetime(data["date"])
            data[self.handler.DEFAULT_PARTITION_FIELD] = data["date"].dt.strftime("%Y%m")
            data = data.reset_index(drop=True)
            self.handler.save_data(
                data=data,
                table=table,
                keys=keys
            )
        elif isinstance(data, Dict):
            self.handler.save_dict(data, table)

    def email_sending(self, title: str, content: str, date: str) -> None:
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
            self.logger.info(f"{date} 研究报告, 邮件发送成功!")
        except Exception as e:
            self.logger.warning(f"{date} 研究报告, 邮件发送失败: {e}")
        finally:
            server.quit()