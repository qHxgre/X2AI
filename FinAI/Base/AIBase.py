
# 基础包
import os
import httpx
import pandas as pd

# 邮件发送
import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# AI
from openai import OpenAI

class BaseAI:
    def __init__(self) -> None:
        """初始化"""
        self.parent_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        # 默认为文件数据库
        self.handler = None

        # ai初始化
        self.client = None
        self.model = None
        self.init_ai()

    def init_ai(self, llms_api: str="deepseek") -> None:
        if llms_api == "deepseek":
            self.client = OpenAI(api_key="sk-7e0d7d183ae84e08b8579a537feff921", base_url="https://api.deepseek.com")
            self.model = "deepseek-chat"
        elif llms_api == "gemini":
            self.client = OpenAI(
                api_key="AIzaSyAuNZ8x72O-lzIeoa_OZKSjlg48P6YBA8E",
                base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
                http_client=httpx.Client(proxy="http://39.104.58.112:31701"),
            )
            self.model = "gemini-2.0-flash-thinking-exp-01-21"
        else:
            raise ValueError("Unsupported LLM API. Please choose 'deepseek' or 'gemini'.")

    def ai_api(self, user_prompt: str, system_prompt: str, json_output: bool=False) -> str:
        if json_output is True:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                stream=False,
                response_format={'type': 'json_object'}
            )
        else:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
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

    def save_md(self, filepath: str, category: str, report_date: str, content: str) -> str:
        """保存内容到指定 Markdown 文件"""
        report_id = "{report_date}_{category}.md".format(
            report_date=report_date,        # 报告日期
            category=category
        )
        with open(os.path.join(filepath, report_id), 'w', encoding='utf-8') as file:
            file.write(content)

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

        server = smtplib.SMTP_SSL('smtp.qq.com', 465, timeout=10)
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, receiver_email, msg.as_string())
        server.quit()