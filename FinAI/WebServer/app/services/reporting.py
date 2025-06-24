import os
import re
from datetime import datetime, timedelta
from flask import jsonify
from Base import LoggerController
from AIBots.SentimentalBot.robot import SentimentalBot
from AIBots.SentimentalBot.templets import TEMPLET_REPORT

class ReportService:
    def __init__(self, bot: SentimentalBot, logger: LoggerController):
        self.bot = bot
        self.logger = logger
        self.logger.info("ReportService initialized")
    
    def get_chart(self, directory: str):
        # 获取目录下所有.html文件
        html_files = [f for f in os.listdir(directory) if f.endswith('.html')]
        # 提取文件名中的日期并转换为datetime对象
        dated_files = []
        date_pattern = re.compile(r'(\d{8})')  # 匹配8位数字的日期
        for file in html_files:
            match = date_pattern.search(file)
            if match:
                date_str = match.group(1)
                try:
                    date = datetime.strptime(date_str, '%Y%m%d')
                    dated_files.append((date, file))
                except ValueError:
                    continue  # 如果日期格式无效，跳过该文件
        
        if not dated_files:
            self.logger.warning("没有找到任何图表文件!")
            return "No chart files found", 404
        
        # 按日期排序并返回最新的文件
        dated_files.sort(reverse=True)
        latest_file = dated_files[0][1]
        last_date = latest_file.replace(".html", "")
        self.logger.info(f"[自动分析] 找到 {len(dated_files)} 个图表文件, 最新日期: {last_date}")
        if last_date != datetime.now().strftime("%Y%m%d"):
            self.logger.warning(f"最新图表日期 {last_date} 与当前日期不一致!")

        filepath = os.path.join(directory, latest_file)
        with open(filepath, 'r', encoding='utf-8') as f:
            html_content = f.read()
        return jsonify({
            "last_date": last_date,
            'data': html_content
        })

    def get_reports(self):
        today = datetime.now()
        start_date = (today - timedelta(days=90)).strftime("%Y-%m-%d")
        end_date = today.strftime("%Y-%m-%d")
        reports = self.bot.handler.read_dict(
            table="aicache_researcher",
            filters={"date": [start_date, end_date]}
        )

        sorted_reports = dict(sorted(reports.items(), key=lambda x: x[0], reverse=True))
        
        formatted_reports = {}
        for date, report in sorted_reports.items():
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
            formatted_reports[date.strftime("%Y-%m-%d")] = report_md
        self.logger.info(f"总共获取了 {len(formatted_reports)} 篇分析报告")
    
        return jsonify({
            'status': 'success',
            'data': formatted_reports
        })