import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Union
from base import PARENT_PATH, AIBase


class SentimentalBot(AIBase):
    """基本面AI研究员
    1. 输入单篇文章进行总结和分析
    2. 将第1步的结论进行最后总结
    """
    def __init__(self, n_days: int=3) -> None:
        super().__init__()
        date_format = "%Y-%m-%d"
        self.start_date = (datetime.now() - timedelta(days=n_days)).strftime(date_format)
        self.end_date = datetime.now().strftime(date_format)

        self.filepath_prompt = PARENT_PATH + "AIBots/SentimentalBot/prompts/"
        self.filepath_cache = PARENT_PATH + "AIBots/SentimentalBot/cache/"
        self.filepath_save = PARENT_PATH + "Reports/"

    def get_articles(self, start_date: Optional[str] = None, end_date: Optional[str]=None) -> list:
        """获取文章数据"""
        sd = self.start_date if start_date is None else start_date
        ed = self.end_date if end_date is None else end_date
        data = self.get_data("aisugar_hisugar", sd, ed)
        data = data.drop_duplicates(subset=["date", "article_id", "title"])     # 删除重复的文章
        data["date"] = data["date"].dt.strftime("%Y-%m-%d")
        data = data.to_dict(orient='records')
        return data

    def analyzing_article(self, article: dict) -> Union[bool, str]:
        """分析一篇文章"""
        # 先读取缓存
        cache_id = f"{self.today}_{article['category'].replace('/', '')}_{article['sub_category'].replace('/', '')}_{article['title'].replace('/', '')}.pkl"
        temp = self.cache(self.filepath_cache+cache_id)
        if temp is not None:
            return True, temp
        
        # 无缓存内容则用AI分析
        user_prompt = """
        * 文章标题: {title}
        * 发布时间: {publish_date}
        * 发布分类: {category}
        * 子分类: {sub_category}
        * 内容简介: {brief}
        * 正文: {content}
        """.format(
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
        temp = json.loads(answer)
        self.cache(self.filepath_cache+cache_id, temp)
        return False, temp

    def assistant(self, data: dict) -> list:
        """研究助理: 收集文章, 总结内容, 给出初步判断"""
        # 逐篇文章分析
        result = []
        article_nums = len(data)
        for i, article in enumerate(data):
            buffer, temp = self.analyzing_article(article)
            result.append(temp)
            if buffer is True:
                print(f"命中缓存: ({i+1} / {article_nums}), 标题: {article['title']}")
            else:
                print(f"AI分析: ({i+1} / {article_nums}), 标题: {article['title']}") 
        return result

    def researcher(self, reports: list) -> str:
        """研究员: 分析研究助理的判断, 给出整体的分析结果"""
        # 汇总助理信息
        user_prompt = ""
        report_prompt = """
        ##  文章标题: {title}
        * 发布时间: {publish_report}
        * 内容总结: {summary}
        * 投资建议: {suggestion}
        """
        for report in reports:
            report_str = report_prompt.format(
                title=report["title"],
                summary=report["summary"],
                suggestion=report["proposal"],
                publish_report=report["date"],
            )
            user_prompt += report_str
            user_prompt += "\n\n"

        # AI分析
        system_prompt = self.read_md(self.filepath_prompt+"researcher.md")
        report = self.api_deepseek(user_prompt, system_prompt)

        # 报告保存
        self.save_md(self.filepath_save, "sentimental", report)
        return report

    def analyzing(self) -> None:
        data = self.get_articles()
        assistant_reports = self.assistant(data)
        research_report = self.researcher(assistant_reports)
        self.email_sending(f"SR 舆情分析报告_{self.end_date}", research_report)
