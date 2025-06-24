import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))
print(PROJECT_ROOT)

import os
import re
import time
import json
import pandas as pd
from datetime import datetime, timedelta
from flask import Flask, render_template, request, Response, jsonify, send_from_directory, send_file
from AIBots.SentimentalBot.robot import SentimentalBot
from X2AI.sugar_ai.Base.base import DBFile, DBSQL

app = Flask(__name__)

bot = SentimentalBot(db=DBFile())
today = datetime.now()
TEMPLET_REPORT = """
日期: {date}

# 投资评级

{rating}

# 报告正文

## 摘要

{overview}

## 利好分析

{bullish}

## 利空分析

{bearish}

## 结论

{conclusion}

# 风险提示

{risk}
"""

@app.route('/')
def home():
    return render_template('auto.html')

@app.route('/auto')
def text_page():
    return render_template('auto.html')

@app.route('/manual')
def image_page():
    return render_template('manual.html')

@app.route('/get_article', methods=['POST'])
def get_article():
    global bot

    data = request.json
    start_date = data.get('startDate')
    end_date = data.get('endDate')

    print(f"收到请求参数：开始日期={start_date}, 结束日期={end_date}")
    
    data = bot.get_articles(start_date, end_date)
    print(f"获取到文章数量: {len(data)}")
    return jsonify({
        'status': 'success',
        'data': data
    })

@app.route('/get_chart')
def get_chart():
    # 读取生成的 HTML 文件
    try:
        # 获取目录下所有.html文件
        directory = f"{PROJECT_ROOT}/WebServer/static/images"
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
        
        # 按日期排序并返回最新的文件
        dated_files.sort(reverse=True)
        latest_file = dated_files[0][1]
        last_date = latest_file.replace(".html", "")

        filepath = os.path.join(directory, latest_file)
        with open(filepath, 'r', encoding='utf-8') as f:
            html_content = f.read()
        return jsonify({
            "last_date": last_date,
            'data': html_content
        })  # 直接返回HTML内容
    except FileNotFoundError:
        return "Chart not found", 404

@app.route('/get_reports')
def get_reports():
    global bot
    global today
    global TEMPLET_REPORT

    start_date = (today - timedelta(days=90)).strftime("%Y-%m-%d")
    end_date = today.strftime("%Y-%m-%d")
    reports = bot.handler.read_dict(
        table="aicache_researcher",
        filters={"date": [start_date, end_date]}
    )

    # 转换为字典并按日期降序排序
    sorted_reports = dict(sorted(reports.items(), key=lambda x: x[0], reverse=True))
    
    # 格式化报告内容
    formatted_reports = {}
    for date, report in sorted_reports.items():
        formatted_reports[date.strftime("%Y-%m-%d")] = TEMPLET_REPORT.format(
            date=date,
            rating=report["rating"],
            overview=report["overview"],
            bullish="\n".join(f"* {k}: {v}" for k, v in report["bullish"].items()),
            bearish="\n".join(f"* {k}: {v}" for k, v in report["bearish"].items()),
            conclusion=report["conclusion"],
            risk="* " + "\n* ".join([i for i in report["risk"]]),
        )

    print(f"总共获取了 {len(formatted_reports)} 篇分析报告")
    return jsonify({
        'status': 'success',
        'data': formatted_reports
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
                buffer, analysis_result = bot.analyzing_article(article)
                processed.append(analysis_result)
                if buffer is True:
                    app.logger.info(f"命中缓存: ({idx + 1} / {total}), 标题: {article.get('title', '无标题')}")
                    time.sleep(2)
                else:
                    app.logger.info(f"AI分析: ({idx + 1} / {total}), 标题: {article.get('title', '无标题')}") 
            
            # 生成最终报告
            final_report = bot.researcher(processed)
            
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
    # app.run(port=5001)