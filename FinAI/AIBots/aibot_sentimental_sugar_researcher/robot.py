import os
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict
from joblib import Parallel, delayed
from Base import BaseAI, DBFile, DBSQL, LoggerController, BaseBuilder
from AIBots.aibot_sentimental_sugar_researcher.templets import TEMPLET_ARTICLE
from AIBots.aibot_sentimental_sugar_researcher.schema import AIBotSentimentalSugarResearcherSchema


class AIBotSentimentalSugarResearcher(BaseAI, BaseBuilder):
    """舆情AI研究员"""
    datasource_id = "aibot_sentimental_sugar_researcher"
    unique_together = ["date", "category", "sub_category", "title"]
    sort_by = [("date", "ascending"), ("category", "ascending"), ("sub_category", "ascending"), ("title", "ascending")]
    indexes = ["date"]
    schema = AIBotSentimentalSugarResearcherSchema

    def __init__(
        self,
        start_date: Optional[str]=None,
        end_date: Optional[str]=None,
        n_days: int=7,
        db: Optional[DBFile]=None,
        llms: str="deepseek",
        cache: bool=True,
    ) -> None:
        super().__init__()
        # 日志打印
        self.logger = LoggerController(
            name="sugar_researcher",
            log_level="INFO",
            console_output=False,
            file_output=True,
            log_file="AIBot_sentimental_sugar_researcher.log",
            when='D'
        )

        # 初始化ai
        self.init_ai(llms_api=llms)

        # 数据库
        if db is None:
            self.handler = DBFile(
                base_path=os.path.join(self.parent_path, "DataBase"),
                log_name="dbfile_sugar",
                log_level="DEBUG"
            )
        else:
            self.handler = db
            

        # 日期范围
        date_format = "%Y-%m-%d"
        self.start_date = (datetime.now() - timedelta(days=n_days)).strftime(date_format) if start_date is None else start_date
        self.end_date = datetime.now().strftime(date_format) if end_date is None else end_date

        # 文件路径
        self.filepath_save = os.path.join(self.parent_path, "Reports")

        # 已分析的文章
        self.cache = cache

        # 系统提示词
        self.filepath_prompt = os.path.join(self.parent_path, "AIBots", "aibot_sentimental_sugar_researcher")
        self.system_prompt = self.read_md(os.path.join(self.filepath_prompt, "prompt.md"))

        # 初始化的日志
        self.logger.info(f"=====>>>>> [初始化] 表名: {self.datasource_id}, AI 模型: {llms}, 数据获取周期：{self.start_date} 至 {self.end_date}")

    def get_articles(self, table: str) -> pd.DataFrame:
        """获取文章数据"""
        data = self.handler.read_dataframe(
            table=table,
            filters={"date": [self.start_date, self.end_date]}
        )
        data["date"] = data["date"].dt.strftime("%Y-%m-%d")
        return data
        
    def filter_articles(self, data: pd.DataFrame) -> list:
        """筛选文章数据"""
        # 删除重复的文章
        data = data.drop_duplicates(subset=["date", "article_id", "title"])
        # 筛选指定类型的数据
        data = data[data["sub_category"].isin(["最新资讯", "日报", "热点研究", "周报"])]
        # 按照日期和文章ID排序
        data = data.sort_values(["date", "article_id"], ascending=[False, False])
        return data.to_dict(orient='records')

    def hit_cache(self, data: pd.DataFrame) -> pd.DataFrame:
        """获取已分析的文章数据数据"""
        cache_data = self.handler.read_dataframe(self.datasource_id, filters={"date": [self.start_date, self.end_date]})
        cache_data["date"] = cache_data["date"].dt.strftime("%Y-%m-%d")
        merged_df = data[self.unique_together].merge(cache_data, on=self.unique_together, how='left', indicator=True)
        result = data[merged_df['_merge'] == 'left_only']
        return result

    def write_data(self, data: pd.DataFrame) -> None:
        """保存分析结构"""
        data["date"] = pd.to_datetime(data["date"])
        data[self.handler.DEFAULT_PARTITION_FIELD] = data["date"].dt.strftime("%Y%m")
        data = data.sort_values(self.unique_together).reset_index(drop=True)
        self.write(data)

    def analyzing_article(self, article: dict) -> Tuple[Dict, str]:
        """分析一篇文章"""
        # 无缓存内容则用AI分析
        user_prompt = TEMPLET_ARTICLE.format(
            title = article["title"],
            brief = article["brief"],
            content = article["content"],
        )
        try:
            answer = self.ai_api(user_prompt=user_prompt, system_prompt=self.system_prompt, json_output=True)
            report = json.loads(answer)
            for col in ["short_forecast", "long_forecast", "confidence"]:
                if report[col] == "":
                    report[col] = np.nan
            for col in ["date", "title", "category", "sub_category"]:
                report[col] = article[col]  # 补充文章信息
            msg = "success"
        except Exception as e:
            # 若 AI 无法回答，则为空
            report = {}
            for col in ["date", "title", "category", "sub_category"]:
                report[col] = article[col]
            report["summary"] = "AI 分析失败, 详见 opinion"
            report["opinion"] = e
            report["short_forecast"] = np.nan
            report["long_forecast"] = np.nan
            report["confidence"] = np.nan
            msg = e
        return report, msg

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
        reports, fail_msg = map(list, zip(*result_zip))
        fail_nums = len([i for i in fail_msg if i != "success"])
        self.logger.info(f"并行处理完成! 分析失败占比: {fail_nums} / {article_nums}")
        return reports

    def serial_analyzing(self, articles: list) -> list:
        """串行分析"""
        article_nums = len(articles)
        self.logger.info(f"共计 {article_nums} 篇文章，串行处理！")
        reports = []
        for i, article in enumerate(articles):
            now = datetime.now()
            report, msg = self.analyzing_article(article)
            if msg != "success" :
                self.logger.warning(f"{article['title']} AI 分析失败: {msg}")
            else:
                self.logger.info(f"AI分析: ({i+1} / {article_nums}), 标题: {article['title']}, 耗时: {datetime.now()-now}")
            reports.append(report)
        return reports

    def analyzing(self, run_parallel: bool=True):
        """分析主函数"""
        # STEP 1: 获取数据
        t0 = datetime.now()
        data = self.get_articles(table="aisugar_hisugar")
        t1 = datetime.now()
        if data.shape[0] > 0:
            self.logger.info(f"数据获取成功: {data.shape}, 耗时: {t1-t0}")
        else:
            self.logger.warning(f"数据获取失败: {data.shape}, 请检查！")
            return

        # STEP 2: 若启用缓存, 则剔除已分析的文章
        if self.cache is True:
            try:
                left_data = self.hit_cache(data)
                t2 = datetime.now()
                if left_data.shape[0] > 0:
                    self.logger.info(f"读取缓存成功, 剔除后已分析文章后, 剩余数据大小: {left_data.shape}, 耗时: {t2-t1}")
                else:
                    self.logger.warning(f"读取缓存成功, 剔除后已分析文章后, 无待分析的文章: {left_data.shape}, 请检查！耗时: {t2-t1}")
                    return
            except Exception as e:
                left_data = data
                t2 = datetime.now()
                self.logger.warning(f"读取缓存失败: {e}, 将使用原数据: {left_data.shape}")
        

        # STEP 3: 数据清洗
        articles = self.filter_articles(left_data)
        t3 = datetime.now()
        if len(articles) == 0:
            self.logger.warning(f"没有可分析的文章: {len(articles)} !")
            return
        self.logger.info(f"数据清洗, 待分析的文章数量: {len(articles)}, 耗时: {t3-t2}")

        # STEP 4: AI分析
        if run_parallel:
            reports = self.parallel_analyzing(articles)
        else:
            reports = self.serial_analyzing(articles)
        t4 = datetime.now()
        if len(reports) == 0:
            self.logger.warning(f"未分析出任何文章: {len(reports)} !")
            return
        self.logger.info(f"AI分析, 报告数据: {len(reports)}, 耗时: {t4-t3}")
        
        # STEP 5: 存储分析结果
        normalized_df = self.normalize(pd.DataFrame(reports))
        normalized_df[self.handler.DEFAULT_PARTITION_FIELD] = normalized_df["date"].dt.strftime("%Y%m")
        self.write(normalized_df)
        t5 = datetime.now()
        self.logger.info(f"存储分析结果: {normalized_df.shape}, 耗时: {t5-t4}") 