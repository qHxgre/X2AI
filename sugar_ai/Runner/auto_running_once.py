import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent
if  str(project_root) not in sys.path:
    sys.path.append(str(project_root))

import time
import schedule
from datetime import datetime, timedelta
from DataBuilder.hisugar.crawler import HigSugarCrawler
from DataBuilder.future_bar1d.builder import FutureBar1dBuilder
from X2AI.sugar_ai.Base.base import DBFile, DBSQL
from AIBots.SentimentalBot.robot import SentimentalBot

# 设置参数
today = datetime.strptime("2025-06-10", "%Y-%m-%d")
# today = datetime.now()
start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
end_date = today.strftime("%Y-%m-%d")
print(f"{datetime.now()} 开始运行: {start_date} 至 {end_date}")

t0 = datetime.now()
# 爬取数据
HigSugarCrawler(
    start_date=start_date,
    end_date=end_date,
    db=DBFile(),
).crawl()
t1 = datetime.now()
print("爬去舆情数据, 耗时: ", t1-t0)

# 获取行情数据
FutureBar1dBuilder(
    start_date=start_date,
    end_date=end_date,
    db=DBFile()
).build()
t2 = datetime.now()
print("获取行情数据, 耗时: ", t2-t1)

# AI 分析
bot = SentimentalBot(db=DBFile(), start_date=start_date, end_date=end_date)
bot.analyzing()
t3 = datetime.now()
print("AI分析, 耗时: ", t3-t2)

print("===>>>> 自动运行完成，等待12个小时！")