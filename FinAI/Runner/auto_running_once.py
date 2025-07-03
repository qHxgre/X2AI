import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent
if  str(project_root) not in sys.path:
    sys.path.append(str(project_root))
print(project_root)

import pandas as pd
from datetime import datetime, timedelta
from DataBuilder.hisugar.crawler import HigSugarCrawler
from DataBuilder.future_bar1d.builder import FutureBar1dBuilder
from AIBots.aibot_sentimental_sugar_researcher.robot import AIBotSentimentalSugarResearcher
from AIBots.aibot_sentimental_sugar_manager.robot import AIBotSentimentalSugarManager
from Base import DBFile

# 设置参数
# start_date='2025-06-28'
# end_date='2025-07-03'
start_date = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
end_date = datetime.now().strftime("%Y-%m-%d")
print(f"{datetime.now()} 开始运行: {start_date} 至 {end_date}")

t0 = datetime.now()
# 爬取数据
HigSugarCrawler(
    start_date=start_date,
    end_date=end_date,
).crawl()
t1 = datetime.now()
print("爬取舆情数据, 耗时: ", t1-t0)

# 获取行情数据
FutureBar1dBuilder(
    start_date=start_date,
    end_date=end_date,
).build()
t2 = datetime.now()
print("获取行情数据, 耗时: ", t2-t1)

# AI 分析
for today in pd.date_range(start=start_date, end=end_date, freq='D'):
    bot = AIBotSentimentalSugarResearcher(
        start_date=today.strftime('%Y-%m-%d'),
        end_date=today.strftime('%Y-%m-%d'),
        llms='deepseek',
        cache=True,
    )

    bot.analyzing()
t3 = datetime.now()
print("[研究员] AI分析, 耗时: ", t3-t2)

for today in pd.date_range(start=start_date, end=end_date, freq='D'):
    bot = AIBotSentimentalSugarManager(
        start_date=today.strftime('%Y-%m-%d'),
        end_date=today.strftime('%Y-%m-%d'),
        llms='deepseek',
        cache=True,
    )
    bot.analyzing()
t4 = datetime.now()
print("[基金经理] AI分析, 耗时: ", t4-t3)