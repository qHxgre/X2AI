import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from AIBots.SentimentalBot.robot import SentimentalBot
from flask import Flask, jsonify, request

app = Flask(__name__)

bot = SentimentalBot()


@app.route('/')
def index():
    return app.send_static_file('index.html')

@app.route('/get_article', methods=['POST'])
def get_article():
    data = request.json
    start_date = data.get('startDate')
    end_date = data.get('endDate')
    user_opinion = data.get('userOpinion')

    print(f"收到请求参数：开始日期={start_date}, 结束日期={end_date}, 用户观点={user_opinion}")
    
    data = bot.get_articles(start_date, end_date)

    return jsonify({
        'status': 'success',
        'data': data
    })

if __name__ == '__main__':
    app.run(debug=True)