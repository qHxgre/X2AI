
import os
import uuid
import pickle
import numpy as np
import pandas as pd
import requests
import pydantic
from datetime import datetime
from typing import Any, Dict, List, Union, Optional


from openai import OpenAI

import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from sqlalchemy import create_engine, text
from sqlalchemy import inspect, MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError


class DBFile:
    """文件类型数据库"""
    def __init__(self):
        self.parent_path = os.path.dirname(os.path.abspath(__file__))

        self.base_path = self.parent_path + '/DataBase/'
        self.category_structure = {
            "国内新闻": [
                '最新资讯', '糖料生产', '食糖生产', '糖厂开工', '食糖产销',
                '海关数据', '现货报价/进口成本', '食糖消费', '白糖期货/期权',
                '糖业政策', '蔗区气象', '副产品/替代品', '产业风采', '通知/公告', '糖业会议/培训',
            ],
            "国际新闻": [
                '最新资讯', '糖料生产', '食糖生产', '食糖贸易', '食糖消费',
                '供需分析', '期货市场', '糖业政策', '环球财经'
            ],
            "行业研究": [
                '热点研究', '日报', '周报', '月报', '国内市场', '国际市场',
                '调研报告', '宏观研究', '权威解读', '季报年报',
                '糖业论文', '糖业科普',
            ]
        }

    def _validate_data(self, df: pd.DataFrame) -> None:
        """验证数据格式和分类有效性"""
        required_columns = {'article_id', 'date', 'category', 'sub_category', 'title', 'brief', 'content'}
        if not required_columns.issubset(df.columns):
            missing = required_columns - set(df.columns)
            raise ValueError(f"缺少必要列: {missing}")

        # 验证分类有效性
        valid_categories = self.category_structure.keys()
        invalid_categories = df[~df['category'].isin(valid_categories)]
        if not invalid_categories.empty:
            raise ValueError(f"无效分类: {invalid_categories['category'].unique().tolist()}")

        # 验证子分类有效性
        for cat, valid_subs in self.category_structure.items():
            cat_data = df[df['category'] == cat]
            invalid_subs = cat_data[~cat_data['sub_category'].isin(valid_subs)]
            if not invalid_subs.empty:
                raise ValueError(f"分类'{cat}'下无效的子分类: {invalid_subs['sub_category'].unique().tolist()}")

    def save_data(self, df: pd.DataFrame, table: str) -> None:
        """保存DataFrame到指定目录结构"""
        self._validate_data(df)
        
        for (date_val, category, sub_category), group_df in df.groupby(['date', 'category', 'sub_category']):
            dir_path = os.path.join(self.base_path, str(date_val.strftime("%Y-%m-%d")), category, sub_category)
            os.makedirs(dir_path, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = os.path.join(dir_path, f"data_{timestamp}.parquet")
            group_df.to_parquet(file_path)

    def read_data(
            self,
            table: str,
            start_date: str,
            end_date: str,
            categories: Optional[List[str]] = None,
            sub_categories: Optional[List[str]] = None
        ) -> pd.DataFrame:
        """根据条件加载数据"""
        base_path = self.base_path + table
        start_date = pd.to_datetime(start_date).date()
        end_date = pd.to_datetime(end_date).date()
        
        collected_files = []
        
        # 遍历目录结构
        for root, dirs, files in os.walk(base_path):
            # 解析路径要素
            path_parts = os.path.relpath(root, base_path).split(os.sep)
            
            # 验证路径深度
            if len(path_parts) != 3 or path_parts[0] == '.':
                continue
            date_str, category, sub_category = path_parts
            
            # 日期过滤
            try:
                current_date = pd.to_datetime(date_str).date()
            except ValueError:
                continue
            
            if (start_date and current_date < start_date) or (end_date and current_date > end_date):
                continue
                
            # 分类过滤
            if categories and category not in categories:
                continue
                
            # 子分类过滤
            if sub_categories and sub_category not in sub_categories:
                continue
                
            # 收集文件
            for file in files:
                if file.endswith(".parquet"):
                    collected_files.append(os.path.join(root, file))
        
        # 读取并合并数据
        if collected_files:
            return pd.concat([pd.read_parquet(f) for f in collected_files], ignore_index=True)
        return pd.DataFrame()


class DBSQL:
    """SQL数据库"""
    def __init__(self):
        """
        初始化数据库连接
        :param username: 数据库用户名
        :param password: 数据库密码
        :param host: 数据库地址
        :param port: 数据库端口
        :param db_name: 数据库名称
        """
        username = "postgres"
        password = "Xx170016"
        host = "localhost"
        port = "5432"
        db_name = "sugari"

        self.engine = create_engine(
            f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{db_name}',
            pool_pre_ping=True
        )
        self._verify_connection()

    def _verify_connection(self):
        """验证数据库连接是否成功"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except SQLAlchemyError as e:
            raise ConnectionError(f"数据库连接失败: {str(e)}")

    def save_data(self, df, table, if_exists='append', schema=None):
        """
        上传DataFrame到PostgreSQL，支持追加写入并处理主键冲突
        :param df: 要上传的DataFrame
        :param table: 目标表名（字符串）
        :param if_exists: 表存在时的处理策略 ('fail', 'replace', 'append')
        :param schema: 数据库模式名（可选）
        """
        try:
            inspector = inspect(self.engine)
            table_exists = inspector.has_table(table, schema=schema)

            if table_exists:
                # 反射表结构（使用新变量名保存Table对象）
                metadata = MetaData(schema=schema)
                try:
                    reflected_table = Table(
                        table, 
                        metadata, 
                        autoload_with=self.engine
                    )
                except SQLAlchemyError as e:
                    raise RuntimeError(f"无法反射表结构: {str(e)}")

                # 获取主键列
                pk_columns = [key.name for key in reflected_table.primary_key]
                has_pk = len(pk_columns) > 0

                # 检查并调整列顺序
                table_columns = [col.name for col in reflected_table.columns]
                if set(df.columns) != set(table_columns):
                    raise ValueError(f"DataFrame列名与表 {table} 不匹配")
                df = df[table_columns].copy()

                if if_exists == 'append':
                    if has_pk:
                        # 执行upsert操作
                        data = df.to_dict(orient='records')
                        chunk_size = 1000

                        for i in range(0, len(data), chunk_size):
                            chunk = data[i:i + chunk_size]
                            stmt = insert(reflected_table).values(chunk)
                            update_dict = {
                                col: getattr(stmt.excluded, col)
                                for col in df.columns
                                if col not in pk_columns
                            }
                            stmt = stmt.on_conflict_do_update(
                                index_elements=pk_columns,
                                set_=update_dict
                            )
                            with self.engine.begin() as conn:
                                conn.execute(stmt)
                        success_msg = f"数据成功更新到表 {schema}.{table}" if schema else f"数据成功更新到表 {table}"
                    else:
                        # 无主键直接追加（使用原始表名字符串变量）
                        df.to_sql(
                            name=table,
                            con=self.engine,
                            schema=schema,
                            if_exists='append',
                            index=False,
                            method='multi'
                        )
                        success_msg = f"数据成功追加到表 {schema}.{table}" if schema else f"数据成功追加到表 {table}"
                elif if_exists == 'replace':
                    df.to_sql(
                        name=table,
                        con=self.engine,
                        schema=schema,
                        if_exists='replace',
                        index=False,
                        method='multi'
                    )
                    success_msg = f"数据成功替换表 {schema}.{table}" if schema else f"数据成功替换表 {table}"
                else:  # 'fail'
                    df.to_sql(
                        name=table,
                        con=self.engine,
                        schema=schema,
                        if_exists='fail',
                        index=False,
                        method='multi'
                    )
                    success_msg = f"数据成功写入表 {schema}.{table}" if schema else f"数据成功写入表 {table}"
                print(success_msg)
            else:
                # 表不存在，直接创建（使用原始表名字符串变量）
                df.to_sql(
                    name=table,
                    con=self.engine,
                    schema=schema,
                    if_exists=if_exists,
                    index=False,
                    method='multi'
                )
                print(f"数据成功创建表 {schema}.{table}" if schema else f"数据成功创建表 {table}")

        except ValueError as ve:
            raise ValueError(f"数据写入失败: {str(ve)}")
        except SQLAlchemyError as se:
            raise RuntimeError(f"数据库操作失败: {str(se)}")

    def read_data(
            self,
            table: str,
            start_date: str,
            end_date: str,
            time_column: str="date", 
            schema=None
        ) -> pd.DataFrame:
        """
        从数据库读取数据
        :param table: 要读取的表名
        :param time_column: 时间列名（可选）
        :param start_date: 开始时间（可选）
        :param end_date: 结束时间（可选）
        :param schema: 数据库模式名（可选）
        :return: 包含查询结果的DataFrame
        """
        try:
            # 构建基础查询
            table_ref = f'"{schema}"."{table}"' if schema else f'"{table}"'
            query = f"SELECT * FROM {table_ref}"
            params = {}
            conditions = []

            # 添加时间过滤条件
            if time_column:
                if start_date or end_date:
                    if start_date:
                        conditions.append(f"{time_column} >= :start_date")
                        params['start_date'] = start_date
                    if end_date:
                        conditions.append(f"{time_column} <= :end_date")
                        params['end_date'] = end_date
                else:
                    print("警告：已指定时间列但未设置时间范围，将返回全部数据")

            # 组合完整查询语句
            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            # 执行查询
            return pd.read_sql_query(
                text(query),
                self.engine,
                params=params,
                parse_dates={time_column: '%Y-%m-%d %H:%M:%S'} if time_column else None
            )

        except SQLAlchemyError as e:
            raise RuntimeError(f"数据读取失败: {str(e)}")
        

class BaseBuilder:
    """数据构建类"""
    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.reindex(columns=self.schema.columns())
        df = df.astype(self.schema.field_type_mapping())
        df = df.fillna(self.schema.field_default_mapping())
        return df

    def write(self, df: pd.DataFrame) -> None:
        self.handler.save_data(df=df, table=self.datasource_id)


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


class BaseCrawler(BaseBuilder):
    """爬虫基础类"""
    def __init__(self) -> None:
        pass
    
    def request(self, url, params, headers):
        return requests.get(url, params=params, headers=headers)
    


class AIBase:
    def __init__(self) -> None:
        """初始化"""
        self.parent_path = os.path.dirname(os.path.abspath(__file__))

        self.today = datetime.now().strftime("%Y%m%d")
        self.time = datetime.now().strftime("%H%M%S")

    def get_data(self, table: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取数据"""
        return self.handler.read_data(table=table, start_date=start_date, end_date=end_date)

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