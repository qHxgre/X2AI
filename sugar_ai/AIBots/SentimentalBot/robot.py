import json
import numpy as np
import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from typing import Optional, Union
from joblib import Parallel, delayed
from base import AIBase
from base import DBFile, DBSQL

from pyecharts.charts import Kline, Line, Grid
from pyecharts import options as opts
from pyecharts.commons.utils import JsCode


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
        self.handler = DBFile() if db is None else db
        date_format = "%Y-%m-%d"
        self.start_date = (datetime.now() - timedelta(days=n_days)).strftime(date_format) if start_date is None else start_date
        self.end_date = datetime.now().strftime(date_format) if end_date is None else end_date

        # 当前日期
        self.today = datetime.now().strftime("%Y%m%d") if self.end_date is None else pd.to_datetime(self.end_date).strftime("%Y%m%d")
        self.time = datetime.now().strftime("%H%M%S")

        self.filepath_prompt = self.parent_path + "/AIBots/SentimentalBot/prompts/"
        self.filepath_save = self.parent_path + "/Reports/"

        # 原始分析数据
        def _get_raw(table) -> pd.DataFrame:
            df = self.handler.read_data(table, filters={"date": [self.start_date, self.end_date]})
            df["date"] = df["date"].dt.strftime("%Y-%m-%d")
            return df
        self.table_assistant = "aicache_assistant"
        self.raw_assistant = self.handler.read_data(self.table_assistant, filters={"date": [self.start_date, self.end_date]})
        self.raw_assistant["date"] = self.raw_assistant["date"].dt.strftime("%Y-%m-%d")
        self.table_researcher = "aicache_researcher"
        self.raw_researcher = self.handler.read_dict(self.table_researcher, filters={"date": [self.start_date, self.end_date]})

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
        params = {
            "date": article["date"],
            "category": article["category"],
            "subcategory": article["sub_category"],
            "title": article["title"],
        }
        temp = self.read_cache(self.raw_assistant, params)
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
            temp["category"] = article["category"]
            temp["subcategory"] = article["sub_category"]
        else:
            # 若deepseek无法回答，则为空
            temp = {
                'title': article["title"],
                'date': article["date"],
                'category': article["category"],
                'subcategory': article["sub_category"],
                'summary': '',
                'opinion': ''
            }
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
                if article["title"] != "赤藓糖醇过剩改善，代糖股业绩回暖，新代糖又现扩产潮":
                    continue
                buffer, temp = self.analyzing_article(article)
                result.append(temp)
                if buffer is True:
                    self.write_log(f"[Assistant] 命中缓存: ({i+1} / {article_nums}), 标题: {article['title']}", logout=1)
                else:
                    self.write_log(f"[Assistant] AI分析: ({i+1} / {article_nums}), 标题: {article['title']}", logout=1) 
        
        # 存储缓存
        self.save_cache(
            data=pd.DataFrame(result),
            table=self.table_assistant,
            keys=["date", "category", "subcategory", "title"]
        )
        return result

    def analyzing_assistant_reports(self, repeorts: list, cache: bool=False) -> dict:
        """分析研究助理的文章"""
        # 读取缓存
        if cache is True:
            temp = self.read_cache(self.raw_researcher, {"date": self.end_date})
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
            temp["date"] = self.end_date
        else:
            temp = None
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

        # 存储缓存
        self.save_cache(
            data={pd.to_datetime(temp["date"]): temp},
            table=self.table_researcher,
        )

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

    def plotting(self, today: Optional[str] = None):
        """分析画图"""
        end_date = today if today is not None else self.end_date
        start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=365)).strftime("%Y-%m-%d")

        # 行情数据
        ohlc_df = self.handler.read_data(
            table="future_bar1d",
            filters={"date": [start_date, end_date]},
            columns=["date", "instrument", "high", "open", "low", "close"]
        )

        # 评级数据
        reports = self.handler.read_dict(
            table="aicache_researcher",
            filters={"date": [start_date, end_date]}
        )
        rating = pd.DataFrame(reports).T.reset_index(drop=True)[["date", "rating"]]
        rating["date"] = pd.to_datetime(rating["date"])

        df = pd.merge(rating, ohlc_df, how="right", on=["date"])
        df = df.dropna(subset=["instrument"]).sort_values('date')
        df["rating"] = df["rating"].replace({"震荡": 0, "上涨": 1, "下跌": -1})
        df = df.set_index("date")

        # 准备K线数据
        kline_data = df[['open', 'close', 'low', 'high']].values.tolist()
        dates = df.index.strftime('%Y-%m-%d').tolist()
        rating = df['rating'].tolist()

        # 构造分段区域填充
        pieces = []
        current = 0
        default_color = 'rgba(128,128,128,0.15)'  # gray for nan/default
        while current < len(rating):
            val = rating[current]
            start = current
            while current + 1 < len(rating) and rating[current + 1] == val:
                current += 1
            end = current
            color = {
                1: 'rgba(255,0,0,0.15)', 
                -1: 'rgba(0,255,0,0.15)', 
                0: 'rgba(0,0,255,0.15)'
            }.get(val, default_color)
            pieces.append({
                "xAxis": [start, end],
                "itemStyle": {"color": color}
            })
            current += 1

        # 创建K线图
        kline = (
            Kline()
            .add_xaxis(dates)
            .add_yaxis(
                "",
                kline_data,
                itemstyle_opts=opts.ItemStyleOpts(
                    color="#ec0000",  # 上涨颜色
                    color0="#00da3c",  # 下跌颜色
                ),
            )
            .set_global_opts(
                title_opts=opts.TitleOpts(title="K线图&AI评级(红色=上涨 | 蓝色=震荡 | 绿色=看跌)"),
                xaxis_opts=opts.AxisOpts(
                    type_="category",
                    is_scale=True,
                    boundary_gap=False,
                    axisline_opts=opts.AxisLineOpts(is_on_zero=False),
                    splitline_opts=opts.SplitLineOpts(is_show=False),
                    split_number=20,
                    min_="dataMin",
                    max_="dataMax",
                ),
                tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="line"),
                datazoom_opts=[
                    opts.DataZoomOpts(
                        is_show=False,
                        type_="inside",
                        xaxis_index=[0],
                        range_start=0,
                        range_end=100,
                    ),
                    opts.DataZoomOpts(
                        is_show=True,
                        xaxis_index=[0],
                        type_="slider",
                        pos_top="90%",
                        range_start=0,
                        range_end=100,
                    ),
                ],
            )
            .set_series_opts(
                markarea_opts=opts.MarkAreaOpts(
                    data=[[{"xAxis": dates[piece["xAxis"][0]]}, {"xAxis": dates[piece["xAxis"][1]], "itemStyle": piece["itemStyle"]}] for piece in pieces]
                )
            )
        )

        # kline.render_notebook()
        filename = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=365)).strftime("%Y%m%d")
        kline.render(self.parent_path+f"/WebServer/static/images/{filename}.html")

    def analyzing(self):
        data = self.get_articles()
        assistant_reports = self.assistant(data, run_parallel=True)
        research_report = self.researcher(assistant_reports)
        self.plotting()
        self.email_sending(f"SR 舆情分析报告_{self.end_date}", research_report)
