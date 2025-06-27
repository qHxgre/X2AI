import os
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict
from joblib import Parallel, delayed
from Base import BaseAI, DBFile, DBSQL, LoggerController, BaseBuilder
from AIBots.SentimentalBot.templets import TEMPLET_ARTICLE
from AIBots.SentimentalBot.schema import SugarSentimentalAssistantSchema


class SentimentalResearcherBot(BaseAI, BaseBuilder):
    """舆情AI研究员"""
    datasource_id = "ai_sugar_sentimental_researcher"
    unique_together = ["date", "instrument"]
    sort_by = [("date", "ascending"), ("instrument", "ascending")]
    indexes = ["date"]
    schema = SugarSentimentalAssistantSchema

    def __init__(
        self,
        start_date: Optional[str]=None,
        end_date: Optional[str]=None,
        n_days: int=7,
        db: Optional[DBFile]=None,
        llms: str="deepseek",
        using_cache: bool=True,
    ) -> None:
        super().__init__()
        # 日志打印
        self.logger = LoggerController(
            name="SentimentalBot",
            log_level="INFO",
            console_output=False,
            file_output=True,
            log_file="AIBot_sugar_sentimental_researcher.log",
            when='D'
        )

        # 初始化ai
        self.init_ai(llms_api=llms)

        # 数据库
        self.handler = DBFile(os.path.join(self.parent_path, "DataBase")) if db is None else db

        # 日期范围
        date_format = "%Y-%m-%d"
        self.start_date = (datetime.now() - timedelta(days=n_days)).strftime(date_format) if start_date is None else start_date
        self.end_date = datetime.now().strftime(date_format) if end_date is None else end_date

        # 文件路径
        self.filepath_prompt = os.path.join(self.parent_path, "AIBots", "SentimentalBot", "prompts")
        self.filepath_save = os.path.join(self.parent_path, "Reports")

        # 获取原始数据
        self.using_cache = using_cache
        if self.using_cache is True:
            self.raw_data = self.get_caches()

        # 初始化的日志
        self.logger.info(f"初始化 ===>>> ai: {llms}, 数据获取周期：{self.start_date} 至 {self.end_date}")

    def get_caches(self) -> pd.DataFrame:
        """获取缓存数据"""
        try:
            raw_data = self.handler.read_data(self.datasource_id, filters={"date": [self.start_date, self.end_date]})
            raw_data["date"] = raw_data["date"].dt.strftime("%Y-%m-%d")
        except Exception as e:
            self.logger.warning(f"[assistant] 读取缓存失败: {e}, 将使用空数据")
            raw_data = pd.DataFrame(columns=["date", "category", "sub_category", "title", "summary", "opinion", "short_forecast", "long_forecast", "confidence"])
        return raw_data

    def get_articles(self, table: str, start_date: Optional[str] = None, end_date: Optional[str]=None) -> pd.DataFrame:
        """获取文章数据"""
        sd = self.start_date if start_date is None else start_date
        ed = self.end_date if end_date is None else end_date
        data = self.get_data(table, sd, ed)
        if data.shape[0] == 0:
            self.logger.warning(f"数据大小: {data.shape}, 数据获取失败，请检查！")
            return pd.DataFrame()
        else:
            self.logger.info(f"数据大小: {data.shape}, 数据获取成功！")
            return data
        
    def filter_articles(self, data: pd.DataFrame) -> list:
        """筛选文章数据"""
        # 删除重复的文章
        data = data.drop_duplicates(subset=["date", "article_id", "title"])
        # 筛选指定类型的数据
        data = data[data["sub_category"].isin(["最新资讯", "日报", "热点研究", "周报"])]
        # 按照日期和文章ID排序
        data = data.sort_values(["date", "article_id"], ascending=[False, False])
        data["date"] = data["date"].dt.strftime("%Y-%m-%d")
        return data.to_dict(orient='records')

 
    def cache(self, data: pd.DataFrame, input_filters: dict) -> Optional[dict]:
        """读取命中缓存
        # input_filters: 输入的过滤器，用于筛选data中是否有符合的数据
        """
        mask = pd.Series(True, index=data.index)
        for k, v in input_filters.items():
            mask &= (data[k] == v)
        
        content = data[mask]
        if content.shape[0] != 0:
            return content.iloc[0].to_dict()
        else:
            return None

    def write_data(self, data: pd.DataFrame) -> None:
        """保存分析结构"""
        data["date"] = pd.to_datetime(data["date"])
        data[self.handler.DEFAULT_PARTITION_FIELD] = data["date"].dt.strftime("%Y%m")
        data = data.sort_values(self.unique_together).reset_index(drop=True)
        self.write(data)

    def analyzing_article(self, article: dict) -> Tuple[bool, Dict]:
        """分析一篇文章"""
        # 缓存
        if self.using_cache is True:
            cache_temp = self.cache(
                self.raw_data,
                input_filters={
                    "date": article["date"],
                    "category": article["category"],
                    "sub_category": article["sub_category"],
                    "title": article["title"],
                }
            )
            if cache_temp is not None:
                return True, cache_temp
        
        # 无缓存内容则用AI分析
        user_prompt = TEMPLET_ARTICLE.format(
            title = article["title"],
            brief = article["brief"],
            content = article["content"],
        )
        sys_prompt = self.read_md(os.path.join(self.filepath_prompt, "assistant.md"))
        try:
            answer = self.ai_api(user_prompt=user_prompt, sys_prompt=sys_prompt, json_output=True)
        except Exception as e:
            self.logger.warning(f"[Assistant] AI 分析失败: {e}")
            answer = None
        
        if answer is not None:
            report = json.loads(answer)
            for col in ["date", "title", "category", "sub_category"]:
                report[col] = article[col]  # 补充文章信息
        else:
            # 若 AI 无法回答，则为空
            report = {}
            for col in ["date", "title", "category", "sub_category"]:
                report[col] = article[col]
            for col in ['summary', 'opinion', 'short_forecast', 'long_forecast', 'confidence']:
                report[col] = ""
        return False, report

    def parallel_analyzing(self, articles: list) -> list:
        """并行分析"""
        def parallel_run(article):
            """并行处理"""
            return  self.analyzing_article(article)

        article_nums = len(articles)
        self.logger.info(f"共计 {article_nums} 篇文章，并行处理！")
        result_zip = Parallel(n_jobs=-1, backend='threading')(
            delayed(parallel_run)(article) for article in articles
        )
        hit_cache, reports = map(list, zip(*result_zip))
        self.logger.info(f"并行处理完成: 命中缓存: {len([i for i in hit_cache if i is not False])} / {article_nums}")
        return reports

    def serial_analyzing(self, articles: list) -> list:
        """串行分析"""
        article_nums = len(articles)
        self.logger.info(f"共计 {article_nums} 篇文章，串行处理！")
        reports = []
        for i, article in enumerate(articles):
            t0 = datetime.now()
            try:
                hit_cache, report = self.analyzing_article(article)
                reports.append(report)
            except Exception as e:
                self.logger.warning(f"{article['title']} AI 分析失败: {e}")
            t1 = datetime.now()
            if hit_cache is True:
                self.logger.info(f"命中缓存: ({i+1} / {article_nums}), 标题: {article['title']}")
            else:
                self.logger.info(f"AI分析: ({i+1} / {article_nums}), 标题: {article['title']}, 耗时: {t1 - t0}")
        return reports


    def analyzing(self, data: dict, run_parallel: bool=False):
        """分析主函数"""
        # 获取数据
        data = self.get_articles(table="aisugar_hisugar")

        # 数据清洗
        articles = self.filter_articles(data)

        if len(data) == 0:
            self.logger.warning(f"没有可分析的文章: {data.shape}, 返回空列表！")
            return None
    
        # 逐篇文章分析
        if run_parallel:
            reports = self.parallel_analyzing(articles)
        else:
            reports = self.serial_analyzing(articles)
        
        # 存储分析结果
        normalized_df = self.normalize(pd.DataFrame(reports))
        normalized_df[self.handler.DEFAULT_PARTITION_FIELD] = normalized_df["date"].dt.strftime("%Y%m")
        normalized_df = normalized_df.sort_values(self.unique_together).reset_index(drop=True)
        self.write(normalized_df)
        self.logger.info(f"存储分析结果, 缓存大小: {normalized_df.shape}") 