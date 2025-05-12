import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

import time
import schedule
from datetime import datetime, timedelta
from DataBuilder.hisugar.crawler import HigSugarCrawler
from base import DBFile, DBSQL

def running():
    # 设置参数
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    print(f"{datetime.now()} 开始运行: {start_date} 至 {end_date}")
    # 爬取数据
    HigSugarCrawler(
        start_date=start_date,
        end_date=end_date,
        db=DBFile()
    ).crawl()
    print("爬取完成，等待6个小时！")

schedule.every(6).hours.do(running)

# 主循环
try:
    while True:
        schedule.run_pending()
        time.sleep(1)
except KeyboardInterrupt:
    print("定时任务已停止")