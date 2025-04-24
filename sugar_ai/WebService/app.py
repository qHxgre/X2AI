import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, Response
from threading import Thread, Event, Lock
from AIBots.SentimentalBot.robot import SentimentalBot

app = Flask(__name__)

bot = SentimentalBot()
current_index = 0
stop_event = Event()
data_lock = Lock()


@app.route('/')
def index():
    return app.send_static_file('index.html')

@app.route('/get_article', methods=['POST'])
def get_article():
    global bot

    data = request.json
    start_date = data.get('startDate')
    end_date = data.get('endDate')
    user_opinion = data.get('userOpinion')

    # print(f"收到请求参数：开始日期={start_date}, 结束日期={end_date}, 用户观点={user_opinion}")
    
    data = bot.get_articles(start_date, end_date)
    articles_data = data
    return jsonify({
        'status': 'success',
        'data': data
    })

@app.route('/analyze', methods=['POST'])
def analyze_articles():
    try:
        data = request.json
        articles = data.get('articles', [])
        user_opinion = data.get('userOpinion', '')
        
        def generate():
            processed = []
            total = len(articles)
            
            # 分析每篇文章
            for idx, article in enumerate(articles):
                progress = {
                    'status': 'processing',
                    'current': idx + 1,
                    'total': total,
                    'title': article.get('title', '无标题')
                }
                yield json.dumps(progress) + "\n"
                
                # 调用分析模块
                analysis_result = bot.analyzing_article(article)
                processed.append(analysis_result)
            
            # 生成最终报告
            final_report = bot.generate_report(
                processed_data=processed,
                user_opinion=user_opinion
            )
            
            yield json.dumps({
                'status': 'success',
                'report': final_report
            }) + "\n"

        return Response(generate(), mimetype='text/event-stream')
    
    except Exception as e:
        app.logger.error(f'分析失败: {str(e)}')
        return jsonify({
            'status': 'error',
            'message': f'分析过程中出现错误: {str(e)}'
        }), 500

if __name__ == '__main__':
    app.run(debug=True)