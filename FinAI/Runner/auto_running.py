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
from Base import DBFile, DBSQL, LoggerController
from AIBots.SentimentalBot.robot import SentimentalBot

logger = LoggerController(
    name="auto_running",
    log_level="INFO",
    console_output=False,
    file_output=True,
    log_file="Runing_autodaily.log",
    when='D'
)


def auto_running():
    """自动运行脚本"""
    # 设置参数
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"{datetime.now()} 开始运行: {start_date} 至 {end_date}")

    t0 = datetime.now()
    # 爬取数据
    HigSugarCrawler(
        start_date=start_date,
        end_date=end_date,
        db=DBFile(),
    ).crawl()
    t1 = datetime.now()
    logger.info("爬取舆情数据, 耗时: ", t1-t0)

    # 获取行情数据
    FutureBar1dBuilder(
        start_date=start_date,
        end_date=end_date,
        db=DBFile()
    ).build()
    t2 = datetime.now()
    logger.info("获取行情数据, 耗时: ", t2-t1)

    # AI 分析
    bot = SentimentalBot(db=DBFile(), start_date=start_date, end_date=end_date)
    bot.analyzing()
    t3 = datetime.now()
    logger.info("AI分析, 耗时: ", t3-t2)

    logger.info("===>>>> 自动运行完成，等待12个小时！")

schedule.every(12).hours.do(auto_running)

# 主循环
try:
    logger.info(f"{datetime.now()} 开始运行啰！")
    while True:
        schedule.run_pending()
        time.sleep(1)
except KeyboardInterrupt:
    logger.info("定时任务已停止")