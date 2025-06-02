# 基础包
import os
import json
import pickle
import numpy as np
import pandas as pd
import requests
import pydantic
from datetime import datetime
from functools import reduce
from typing import Any, Dict, List, Union, Optional

# 文件数据库
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from pyarrow import compute as pc

# SQL 数据库
from sqlalchemy import create_engine, text
from sqlalchemy import inspect, MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

# 邮件发送
import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# AI
from openai import OpenAI



class DBFile:
    """通用文件数据库基类，处理基于目录结构的文件存储"""
    
    DEFAULT_PARTITION_FIELD = "partitionBy"
    
    def __init__(self, base_path: Optional[str] = None):
        """
        初始化文件数据库
        
        :param base_path: 数据库根目录路径，如果为None则使用项目目录下的DataBase
        """
        if base_path is None:
            self.base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "DataBase")
        else:
            self.base_path = base_path
            
        os.makedirs(self.base_path, exist_ok=True)

    def save_data(
        self,
        data: pd.DataFrame,
        table: str,
        keys: List[str],
    ) -> None:
        """
        将DataFrame保存为Parquet格式，支持分区存储和去重
        
        Args:
            data: 要存储的DataFrame
            table: 表名，将作为子目录名称
            keys: 唯一键列名列表，用于去重
            partition_cols: 分区列名列表，如果为None则不分区
        """
        # 创建表目录
        table_path = os.path.join(self.base_path, table)
        os.makedirs(table_path, exist_ok=True)
        
        # 去重
        if keys:
            data = data.drop_duplicates(subset=keys, keep="last")
        
        # 转换为Arrow Table
        arrow_table = pa.Table.from_pandas(data)
        
        # 保存数据
        if self.DEFAULT_PARTITION_FIELD in data.columns:
            partition_cols = self.DEFAULT_PARTITION_FIELD
            pq.write_to_dataset(
                table=arrow_table,      # 要写入的 Arrow 表
                root_path=table_path,       # 输出目录的根路径
                partition_cols=[self.DEFAULT_PARTITION_FIELD],  # 用于分区的列名列表
                existing_data_behavior="overwrite_or_ignore"        # 如何处理现有数据 ('error', 'overwrite_or_ignore', 'delete_matching')
            )
        else:
            partition_cols = None
            pq.write_table(
                table=arrow_table,
                where=os.path.join(table_path, "data.parquet"),
            )
        
        # 保存元数据
        self._save_metadata(
            table_name=table,
            keys=keys,
            partition_cols=partition_cols,
            schema=arrow_table.schema
        )

    def read_data(
        self,
        table: str,
        filters: Optional[Dict[str, List[Any]]] = None,
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        从Parquet文件中读取数据
        
        Args:
            table: 表名
            filters: 过滤条件字典，支持两种格式:
                    1. {"date": [start_date, end_date]} - 范围筛选
                    2. {"age": [45, 46]} - 多值筛选
            columns: 要读取的列名列表，None表示读取所有列
            
        Returns:
            读取的DataFrame
        """
        table_path = os.path.join(self.base_path, table)
        
        if not os.path.exists(table_path):
            raise FileNotFoundError(f"Table {table} not found")
        
        # 读取元数据
        metadata = self._load_metadata(table)
        
        # 转换筛选条件为PyArrow格式
        arrow_filters = self._convert_filters(filters) if filters else None
        
        # 检查是否是分区表
        if metadata['partition_cols']:
            dataset = ds.dataset(
                source=table_path,
                format="parquet",
                partitioning=[self.DEFAULT_PARTITION_FIELD]
            )
            scanner = dataset.scanner(filter=arrow_filters, columns=columns)
            table = scanner.to_table()
        else:
            table = pq.read_table(
                os.path.join(table_path, "data.parquet"),
                filters=arrow_filters,
                columns=columns
            )
        
        return table.to_pandas()
    
    def _convert_filters(
        self, 
        filters: Dict[str, Union[Dict[str, Any], List[Any]]]
    ) -> pc.Expression:
        """
        将字典形式的筛选条件转换为PyArrow表达式
        
        Args:
            filters: 过滤条件字典
            
        Returns:
            PyArrow表达式对象
        """
        expressions = []
        
        for column, condition in filters.items():
            if column == "date" and isinstance(condition, list):
                # 处理日期范围筛选 [start, end]
                if len(condition) == 2:
                    expr = (pc.field(column) >= pd.to_datetime(condition[0])) & (pc.field(column) <= pd.to_datetime(condition[1]))
                    expressions.append(expr)
                elif len(condition) == 1:
                    expressions.append(pc.field(column) >= pd.to_datetime(condition[0]))
                else:
                    raise ValueError("时间筛选参数错误！")
            elif isinstance(condition, list):
                # 处理多值筛选 [val1, val2, ...]
                if condition:
                    expressions.append(pc.field(column).isin(condition))
            elif isinstance(condition, dict):
                # 处理范围筛选 {"start": x, "end": y}
                expr = None
                if "start" in condition:
                    expr = pc.field(column) >= condition["start"]
                if "end" in condition:
                    end_expr = pc.field(column) <= condition["end"]
                    expr = end_expr if expr is None else expr & end_expr
                if expr is not None:
                    expressions.append(expr)
            else:
                # 处理单值筛选
                expressions.append(pc.field(column) == condition)
        
        # 将所有表达式用AND连接
        if not expressions:
            return None
        elif len(expressions) == 1:
            return expressions[0]
        else:
            # 使用reduce和&运算符组合所有表达式
            return reduce(lambda x, y: x & y, expressions)

    def _save_metadata(
        self,
        table_name: str,
        keys: List[str],
        partition_cols: Optional[List[str]],
        schema: pa.Schema
    ) -> None:
        """保存表的元数据"""
        metadata = {
            "table_name": table_name,
            "keys": keys,
            "partition_cols": partition_cols,
            "schema": {
                "names": schema.names,
                "types": [str(field.type) for field in schema]
            },
            "created_at": pd.Timestamp.now().isoformat()
        }
        
        metadata_path = os.path.join(self.base_path, table_name, "_metadata.json")
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)

    def _load_metadata(self, table_name: str) -> Dict[str, Any]:
        """加载表的元数据"""
        metadata_path = os.path.join(self.base_path, table_name, "_metadata.json")
        
        if not os.path.exists(metadata_path):
            raise FileNotFoundError(f"Metadata for table {table_name} not found")
        
        with open(metadata_path, 'r') as f:
            return json.load(f)

    def list_tables(self) -> List[str]:
        """列出所有表名"""
        return [d for d in os.listdir(self.base_path) 
                if os.path.isdir(os.path.join(self.base_path, d)) and not d.startswith('_')]

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        return os.path.exists(os.path.join(self.base_path, table_name))

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
        self.handler.save_data(
            data=df,
            table=self.datasource_id,
            keys=self.unique_together
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
            print(e)
            return None

        answer = response.choices[0].message.content
        return answer

    def read_md(self, filepath: str) -> str:
        """读取指定 Markdown 文件"""
        with open(filepath, 'r', encoding='utf-8') as file:
            content = file.read()
        return content

    def save_md(self, filepath: str, category: str, report_date: str, publish_time: str, content: str) -> str:
        """保存内容到指定 Markdown 文件"""
        report_id = "{report_date}_{publish_time}_{category}.md".format(
            report_date=report_date,        # 报告日期
            publish_time=publish_time,      # 发表日期
            category=category
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