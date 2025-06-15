import json
import time
from flask import jsonify
from AIBots.SentimentalBot.robot import SentimentalBot
from Base import LoggerController

class AnalysisService:
    def __init__(self, bot: SentimentalBot, logger: LoggerController):
        self.bot = bot
        self.logger = logger
        self.logger.info("AnalysisService initialized")
    
    def get_articles(self, start_date, end_date):
        self.logger.info(f"收到请求参数：开始日期={start_date}, 结束日期={end_date}")
        data = self.bot.get_articles(start_date, end_date)
        self.logger.info(f"[手动分析] 获取到文章数量: {len(data)}")
        return jsonify({
            'status': 'success',
            'data': data
        })
    
    def analyze_articles(self, articles):
        print(f"[手动分析] 开始分析文章数量: {len(articles)}")
        processed = []
        total = len(articles)
        
        for idx, article in enumerate(articles):
            progress = {
                'status': 'processing',
                'current': idx + 1,
                'total': total,
                'title': article.get('title', '无标题')
            }
            yield json.dumps(progress) + "\n"
            
            buffer, analysis_result = self.bot.analyzing_article(article)
            processed.append(analysis_result)
            if buffer is True:
                self.logger.info(f"[手动分析] 命中缓存: ({idx + 1} / {total}), 标题: {article.get('title', '无标题')}")
                time.sleep(2)
            else:
                self.logger.info(f"[手动分析] AI分析: ({idx + 1} / {total}), 标题: {article.get('title', '无标题')}") 
        
        final_report = self.bot.researcher(processed)
        yield json.dumps({
            'status': 'success',
            'report': final_report
        }) + "\n"