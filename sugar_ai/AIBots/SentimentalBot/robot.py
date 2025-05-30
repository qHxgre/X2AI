import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Union
from joblib import Parallel, delayed
from base import AIBase
from base import DBFile, DBSQL


TEMPLET_ARTICLE = """
* 文章标题: {title}
* 发布时间: {publish_date}
* 发布分类: {category}
* 子分类: {sub_category}
* 内容简介: {brief}
* 正文: {content}
"""

TEMPLET_ASSISTANT_REPORT = """
## 第 {i} 篇文章
* 文章标题: {title}
* 发布时间: {publish_report}
* 内容总结: {summary}
* 个人观点: {suggestion}
"""


TEMPLET_REPORT = """
日期: {date}

# 投资评级

{rating}

# 报告正文

## 摘要

{overview}

## 利好分析

{bullish}

## 利空分析

{bearish}

## 结论

{conclusion}

# 风险提示

{risk}
"""

class SentimentalBot(AIBase):
    """基本面AI研究员
    1. 输入单篇文章进行总结和分析
    2. 将第1步的结论进行最后总结
    """
    def __init__(self, start_date: Optional[str]=None, end_date: Optional[str]=None, n_days: int=7, db=None) -> None:
        super().__init__()
        self.handler = DBSQL() if db is None else db
        date_format = "%Y-%m-%d"
        self.start_date = (datetime.now() - timedelta(days=n_days)).strftime(date_format) if start_date is None else start_date
        self.end_date = datetime.now().strftime(date_format) if end_date is None else end_date

        # 当前日期
        self.today = datetime.now().strftime("%Y%m%d") if self.end_date is None else pd.to_datetime(self.end_date).strftime("%Y%m%d")
        self.time = datetime.now().strftime("%H%M%S")

        self.filepath_prompt = self.parent_path + "/AIBots/SentimentalBot/prompts/"
        self.filepath_cache = self.parent_path + "/AIBots/SentimentalBot/cache/"
        self.filepath_save = self.parent_path + "/Reports/"

    def write_log(self, log: str, logout: int=1):
        if logout == 1:
            print(log)

    def get_articles(self, start_date: Optional[str] = None, end_date: Optional[str]=None) -> list:
        """获取文章数据"""
        sd = self.start_date if start_date is None else start_date
        ed = self.end_date if end_date is None else end_date
        data = self.get_data("aisugar_hisugar", sd, ed)
        if data.shape[0] == 0:
            return []
        data = data.drop_duplicates(subset=["date", "article_id", "title"])     # 删除重复的文章
        data = data.sort_values(["date", "article_id"], ascending=[False, False])  # 按照日期和文章ID排序
        data["date"] = data["date"].dt.strftime("%Y-%m-%d")
        data = data.to_dict(orient='records')
        return data

    def analyzing_article(self, article: dict):
        """分析一篇文章"""
        # 缓存
        cache_path = self.filepath_cache + "assistant/"
        cache_id = "{date}_{category}_{sub_category}_{title}.pkl".format(
            date=article["date"].replace("-", ""),
            category=article["category"].replace("/", ""),
            sub_category=article["sub_category"].replace("/", ""),
            title=article["title"].replace("/", "")
        )
        
        temp = self.cache(cache_path+cache_id)
        if temp is not None:
            return True, temp
        
        # 无缓存内容则用AI分析
        user_prompt = TEMPLET_ARTICLE.format(
            title = article["title"],
            publish_date = article["date"],
            category = article["category"],
            sub_category = article["sub_category"],
            brief = article["brief"],
            content = article["content"],
        )

        # 通过AI给出分析结论
        system_prompt = self.read_md(self.filepath_prompt+"assistant.md")
        answer = self.api_deepseek(user_prompt, system_prompt, True)
        
        # 缓存内容
        if answer is not None:
            temp = json.loads(answer)
        else:
            # 若deepseek无法回答，则为空
            temp = {
                'title': article["title"],
                'date': article["date"],
                'summary': '',
                'opinion': ''
            }
        self.cache(cache_path+cache_id, temp)
        return False, temp

    def assistant(self, data: dict, run_parallel: bool=False) -> list:
        """研究助理: 收集文章, 总结内容, 给出初步判断"""
        def parallel_run(i, article, article_nums):
            """并行处理"""
            buffer, temp = self.analyzing_article(article)
            if buffer is True:
                self.write_log(f"[Assistant] 命中缓存: ({i+1} / {article_nums}), 标题: {article['title']}", logout=1)
            else:
                self.write_log(f"[Assistant] AI分析: ({i+1} / {article_nums}), 标题: {article['title']}", logout=1)
            return temp
    
        if len(data) == 0:
            self.write_log("[Assistant] 没有可分析的文章！")
            return []
    
        # 逐篇文章分析
        article_nums = len(data)
        if run_parallel:
            # 并行处理
            self.write_log(f"[Assistant] 共计 {article_nums} 篇文章，并行处理！", logout=1)
            result = Parallel(n_jobs=-1, backend='loky')(
                delayed(parallel_run)(i, article, article_nums) for i, article in enumerate(data)
            )
        else:
            # 串行处理
            self.write_log(f"[Assistant] 共计 {article_nums} 篇文章，串行处理！", logout=1)
            result = []
            for i, article in enumerate(data):
                buffer, temp = self.analyzing_article(article)
                result.append(temp)
                if buffer is True:
                    self.write_log(f"[Assistant] 命中缓存: ({i+1} / {article_nums}), 标题: {article['title']}", logout=1)
                else:
                    self.write_log(f"[Assistant] AI分析: ({i+1} / {article_nums}), 标题: {article['title']}", logout=1) 
        return result

    def analyzing_assistant_reports(self, repeorts: list) -> dict:
        """分析研究助理的文章"""
        # 缓存
        cache_path = self.filepath_cache + "research/"
        cache_id = f"{self.today}_sentimental_researcher_report.pkl"
        
        # 读取缓存
        temp = self.cache(cache_path+cache_id)
        if temp is not None:
            return True, temp

        # 筛选出无效的报告
        repeorts = [i for i in repeorts if i["summary"] != "" and i["opinion"] != ""]

        # 汇总助理收集的文章
        user_prompt = ""
        for i, report in enumerate(repeorts):
            report_str = TEMPLET_ASSISTANT_REPORT.format(
                i=i+1,
                title=report["title"],
                summary=report["summary"],
                suggestion=report["opinion"],
                publish_report=report["date"],
            )
            user_prompt += report_str
            user_prompt += "\n\n"

        # AI分析
        system_prompt = self.read_md(self.filepath_prompt+"researcher.md")
        answer = self.api_deepseek(user_prompt, system_prompt, True)


        # 缓存内容
        if answer is not None:
            temp = json.loads(answer)
            temp["date"] = self.today
        else:
            # 若deepseek无法回答，则为空
            temp = None
        self.cache(cache_path+cache_id, temp)
        return False, temp

    def researcher(self, input_reports: list) -> str:
        """研究员: 分析研究助理的判断, 给出整体的分析结果"""
        if len(input_reports) == 0:
            self.write_log("[Researcher] 没有可分析的报告！")
            return ""
        # AI分析
        buffer, temp = self.analyzing_assistant_reports(input_reports)
        if buffer is True:
            self.write_log(f"[Researcher] 生成 {self.today} 的报告：命中缓存！")
        else:
            self.write_log(f"[Researcher] 生成 {self.today} 的报告：AI分析！")

        # 生成报告
        report = TEMPLET_REPORT.format(
            date=temp["date"],
            rating=temp["rating"],
            overview=temp["overview"],
            bullish="\n".join(f"* {k}: {v}" for k, v in temp["bullish"].items()),
            bearish="\n".join(f"* {k}: {v}" for k, v in temp["bearish"].items()),
            conclusion=temp["conclusion"],
            risk="* " + "\n* ".join([i for i in temp["risk"]]),
        )

        # 保存报告
        report_date = self.end_date.replace("-", "")
        publish_time = f"{self.today}{self.time}"
        self.save_md(self.filepath_save, "sentimental", report_date, publish_time, report)
        return report

    def get_ratings(self, start_date: str, end_date: str) -> dict:
        """获取指定日期范围内的评级"""
        cache_path = self.filepath_cache + "research/"
        import os
        pkl_files = [f for f in os.listdir(cache_path) if f.endswith('.pkl')]
        ratings = {}
        for file in pkl_files:
            answer = self.cache(cache_path+file)
            if (pd.to_datetime(answer["date"]) >= pd.to_datetime(start_date)) and (pd.to_datetime(answer["date"]) <= pd.to_datetime(end_date)):
                # 筛选出符合日期范围的报告
                ratings[answer["date"]] = answer["rating"]
        return ratings

    def analyzing(self):
        data = self.get_articles()
        assistant_reports = self.assistant(data, run_parallel=True)
        research_report = self.researcher(assistant_reports)
        # self.email_sending(f"SR 舆情分析报告_{self.end_date}", research_report)
