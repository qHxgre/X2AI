import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

import os
import time
import json
from datetime import datetime
from flask import Flask, render_template, request, Response, jsonify, send_from_directory
from AIBots.SentimentalBot.robot import SentimentalBot
from base import DBFile, DBSQL

app = Flask(__name__)

bot = SentimentalBot(db=DBFile())


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



@app.route('/api/get_images/')
def get_image():
    filename = "example2.png"
    filepath = os.path.join(f"{PROJECT_ROOT}/WebServer/static/images", filename)
    if not os.path.exists(filepath):
        return jsonify({'status': 'error', 'message': '图片不存在'}), 404
    
    # 返回图片URL
    return jsonify({
        'status': 'success',
        'images': [{
            'url': "/static/images/example2.png",
            'name': '示例图片'
        }]
    })

# 修正静态图片服务路由
@app.route('/static/images/<filename>')
def serve_image(filename):
    return send_from_directory(f"{PROJECT_ROOT}/WebServer/static/images", filename)

@app.route('/api/get_markdown')
def get_markdown():
    today = datetime.now().strftime("%Y%m%d")
    today = "20241201"
    # 解析文件目录，获取指定日期最新分析
    result = {}
    for i in os.listdir(f"{PROJECT_ROOT}/Reports"):
        if i.split('_')[0] == today:
            result[i] = i.split('_')[1]
    filename = max(result, key=result.get)
    filepath = os.path.join(f"{PROJECT_ROOT}/Reports", filename)
    try:
        content = ""
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
        
        return jsonify({'content': content})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


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