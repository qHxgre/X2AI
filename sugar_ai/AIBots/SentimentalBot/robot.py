import os
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Dict
from joblib import Parallel, delayed
from Base import BaseAI, DBFile, DBSQL, LoggerController
from AIBots.SentimentalBot.templets import TEMPLET_ARTICLE, TEMPLET_ASSISTANT_REPORT, TEMPLET_REPORT

from pyecharts.charts import Kline
from pyecharts import options as opts



class SentimentalBot(BaseAI):
    """基本面AI研究员
    1. 输入单篇文章进行总结和分析
    2. 将第1步的结论进行最后总结
    """
    def __init__(self, start_date: Optional[str]=None, end_date: Optional[str]=None, n_days: int=7, db=None) -> None:
        super().__init__()
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
        self.table_assistant = "aicache_assistant"
        self.raw_assistant = self.handler.read_data(self.table_assistant, filters={"date": [self.start_date, self.end_date]})
        self.raw_assistant["date"] = self.raw_assistant["date"].dt.strftime("%Y-%m-%d")
        self.table_researcher = "aicache_researcher"
        self.raw_researcher = self.handler.read_dict(self.table_researcher, filters={"date": [self.start_date, self.end_date]})

        # 日志打印
        self.logger = LoggerController(
            name="SentimentalBot",
            log_level="INFO",
            console_output=False,
            file_output=True,
            log_file="AIBot_sentimental.log",
            when='D'
        )

        # 初始化的日志
        self.logger.info(f"本次AI分析的创建时间为: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"数据获取周期为：{self.start_date} 至 {self.end_date}")


    def get_articles(self, start_date: Optional[str] = None, end_date: Optional[str]=None) -> list:
        """获取文章数据"""
        sd = self.start_date if start_date is None else start_date
        ed = self.end_date if end_date is None else end_date
        data = self.get_data("aisugar_hisugar", sd, ed)
        if data.shape[0] == 0:
            self.logger.warning(f"数据大小: {data.shape}, 数据获取失败，请检查！")
            return []
        else:
            self.logger.info(f"数据大小: {data.shape}, 数据获取成功！")
            data = data.drop_duplicates(subset=["date", "article_id", "title"])     # 删除重复的文章
            data = data.sort_values(["date", "article_id"], ascending=[False, False])  # 按照日期和文章ID排序
            data["date"] = data["date"].dt.strftime("%Y-%m-%d")
            data = data.to_dict(orient='records')
            return data

    def assistant(self, data: dict, run_parallel: bool=False) -> List:
        """研究助理: 收集文章, 总结内容, 给出初步判断"""
        def _analyzing_article(article: dict) -> Tuple[bool, Dict]:
            """分析一篇文章"""
            # 缓存
            cache_temp = self.read_cache(
                self.raw_assistant,
                input_filters={
                    "date": article["date"],
                    "category": article["category"],
                    "subcategory": article["sub_category"],
                    "title": article["title"],
                }
            )
            if cache_temp is not None:
                return True, cache_temp
            
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
            system_prompt = self.read_md(os.path.join(self.filepath_prompt, "assistant.md"))
            answer = self.api_deepseek(user_prompt, system_prompt, True)
            
            # 缓存内容
            if answer is not None:
                report = json.loads(answer)
                report["category"] = article["category"]
                report["subcategory"] = article["sub_category"]
            else:
                # 若deepseek无法回答，则为空
                report = {
                    'title': article["title"],
                    'date': article["date"],
                    'category': article["category"],
                    'subcategory': article["sub_category"],
                    'summary': '',
                    'opinion': ''
                }
                self.logger.debug(f"AI 分析失败: {article['title']}, 返回空内容！")
            return False, report

        def parallel_run(article):
            """并行处理"""
            return  _analyzing_article(article)
    
        if len(data) == 0:
            self.logger.warning("[Assistant] 没有可分析的文章: {data.shape}, 返回空列表！")
            return []
    
        # 逐篇文章分析
        article_nums = len(data)
        if run_parallel:
            # 并行处理
            self.logger.info(f"[Assistant] 共计 {article_nums} 篇文章，并行处理！")
            result_zip = Parallel(n_jobs=-1, backend='loky')(
                delayed(parallel_run)(article) for i, article in enumerate(data)
            )
            hit_static, result = map(list, zip(*result_zip))
            self.logger.info(f"[Assistant] 并行处理完成: 命中缓存: {len([i for i in hit_static if i is not None])} / {article_nums}")
        else:
            # 串行处理
            self.logger.info(f"[Assistant] 共计 {article_nums} 篇文章，串行处理！")
            result = []
            for i, article in enumerate(data):
                hit_cache, report = _analyzing_article(article)
                result.append(report)
                if hit_cache is True:
                    self.logger.info(f"[Assistant] 命中缓存: ({i+1} / {article_nums}), 标题: {article['title']}")
                else:
                    self.logger.info(f"[Assistant] AI分析: ({i+1} / {article_nums}), 标题: {article['title']}") 
        
        # 存储缓存
        self.save_cache(
            data=pd.DataFrame(result),
            table=self.table_assistant,
            keys=["date", "category", "subcategory", "title"]
        )
        self.logger.debug(f"[Assistant] 缓存分析结果: {self.table_assistant}, 缓存大小: {len(result)}") 
        return result

    def researcher(self, input_reports: list, cache: bool) -> str:
        """研究员: 分析研究助理的判断, 给出整体的分析结果"""
        def _analyzing_assistant_reports(input_reports: list, cache: bool) -> Tuple[bool, Optional[Dict]]:
            """分析研究助理的文章"""
            # 读取缓存
            if cache is True:
                # 命中缓存
                cache_temp = self.read_cache(self.raw_researcher, {"date": self.end_date})
                if cache_temp is not None:
                    return True, cache_temp

            # 筛选出无效的报告
            articles = [i for i in input_reports if i["summary"] != "" and i["opinion"] != ""]

            # 汇总助理收集的文章
            user_prompt = ""
            for i, article in enumerate(articles):
                article_str = TEMPLET_ASSISTANT_REPORT.format(
                    i=i+1,
                    title=article["title"],
                    publish_date=article["date"],
                    summary=article["summary"],
                    suggestion=article["opinion"]
                )
                user_prompt += article_str
                user_prompt += "\n\n"

            # AI分析
            system_prompt = self.read_md(os.path.join(self.filepath_prompt, "researcher.md"))
            answer = self.api_deepseek(user_prompt, system_prompt, True)

            # 缓存内容
            if answer is not None:
                report = json.loads(answer)
                report["date"] = self.end_date
            else:
                report = None
                self.logger.debug(f"AI 分析失败: {article['title']}, 返回空内容！")
            return False, report

        if len(input_reports) == 0:
            self.logger.warning(f"[Researcher] 没有可分析的报告: {len(input_reports)}, 返回空字符串！")
            return ""
    
        # AI分析
        hit_cache, report = _analyzing_assistant_reports(input_reports, cache)
        if hit_cache is True:
            self.logger.info(f"[Researcher] 生成 {self.end_date} 的报告：命中缓存！")
        else:
            self.logger.info(f"[Researcher] 生成 {self.end_date} 的报告：AI分析！")

        # 存储缓存
        self.save_cache(
            data={self.end_date: report},
            table=self.table_researcher,
        )

        # 生成报告
        report_md = TEMPLET_REPORT.format(
            analyze_date=report["date"],
            publish_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            rating=report["rating"],
            overview=report["overview"],
            bullish="\n".join(f"* {k}: {v}" for k, v in report["bullish"].items()),
            bearish="\n".join(f"* {k}: {v}" for k, v in report["bearish"].items()),
            conclusion=report["conclusion"],
            risk="* " + "\n* ".join([i for i in report["risk"]]),
        )

        # 保存报告
        report_date = self.end_date.replace("-", "")
        self.save_md(self.filepath_save, "sentimental", report_date, report_md)
        self.logger.info(f"[Researcher] 报告已保存: {report_date}_sentimental.md, 保存路径: {self.filepath_save}")
        return report_md

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
            val = rating[current] if not pd.isna(rating[current]) else None
            start = current
            end = current + 1
            if end >= len(rating):
                break
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
        filepath = os.path.join(self.parent_path, "WebServer", "static", "images")
        filename = (datetime.strptime(end_date, "%Y-%m-%d")).strftime("%Y%m%d")
        kline.render(path=filepath+f"/{filename}.html")
        self.logger.info(f"[Plotting] K线图已保存至: {filepath}/{filename}.html")

    def analyzing(self):
        data = self.get_articles()
        assistant_reports = self.assistant(data, run_parallel=True)
        research_report = self.researcher(assistant_reports, cache=True)
        self.plotting()
        self.email_sending(f"SR 舆情分析报告_{self.end_date}", research_report, self.end_date)
