
    #     # 分阶段处理流程
    #     processed = []
    #     total = len(articles)
    #     for idx, article in enumerate(articles): 
    #         app.logger.debug(f"分析文章: {article['title']}")
    #         progress = {
    #             'status': 'processing',
    #             'current': idx + 1,
    #             'total': total,
    #             'title': article.get('title', '无标题')
    #         }
    #         yield json.dumps(progress) + "\n"

    #         buffer, temp = bot.analyzing_article(article)
    #         processed.append(temp)

    #     # 生成最终报告
    #     final_report = bot.researcher(processed)
    #     # yield json.dumps({
    #     #     'status': 'success',
    #     #     'data': final_report
    #     # })

    # except GeneratorExit:
    #     # 当客户端断开连接时触发
    #     app.logger.info("客户端中止了请求")
    # except Exception as e:
    #     app.logger.error(f'处理失败: {str(e)}')
    #     # yield json.dumps({'status': 'error', 'message': str(e)})

前端代码：
<!DOCTYPE html>
<html lang="zh-CN">

<head>
    <meta charset="UTF-8">
    <title>白糖期货分析平台</title>
    <style>
        :root {
            --primary-color: #2c3e50;
            --secondary-color: #3498db;
            --background: #f5f6fa;
        }

        body {
            font-family: 'Segoe UI', system-ui;
            margin: 0;
            padding: 20px;
            background: var(--background);
        }

        .container {
            max-width: 1200px;
            margin: 0 auto;
            display: grid;
            grid-template-columns: 300px 1fr;
            gap: 20px;
        }

        .control-panel {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        }

        .news-list {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
            height: 500px;          /* 固定容器高度 */
            overflow-y: auto;       /* 垂直方向内容溢出时显示滚动条 */
        }

        .news-item {
            border: 1px solid #eee;
            border-radius: 6px;
            margin-bottom: 10px;
            overflow: hidden;
        }

        .news-header {
            position: relative;
            padding-right: 40px;
        }

        .news-header::after {
            content: '▼';
            position: absolute;
            right: 15px;
            top: 50%;
            transform: translateY(-50%);
            transition: transform 0.3s;
            font-size: 0.8em;
            color: #666;
        }

        .news-item.active .news-header::after {
            transform: translateY(-50%) rotate(180deg);
        }

        .news-item.active .news-content {
            max-height: 1000px;
            padding: 16px;
        }

        .news-content {
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.3s ease-out;
            padding: 0 16px;
            border-top: 1px solid #eee;
        }

        input,
    
        .date-picker {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 12px;
        }

        textarea {
            width: 100%;
            padding: 10px 12px;
            border: 2px solid #e0e0e0;
            border-radius: 6px;
            font-family: inherit;
            font-size: 14px;
            transition: all 0.2s ease;
            background: white;
            color: var(--primary-color);
        }

        /* 输入框聚焦状态 */
        input[type="date"]:focus,
        textarea:focus {
            outline: none;
            border-color: var(--secondary-color);
            box-shadow: 0 0 0 3px rgba(52, 152, 219, 0.1);
        }

        /* 日期输入框自定义图标 */
        input[type="date"]::-webkit-calendar-picker-indicator {
            filter: invert(0.5);
            cursor: pointer;
        }

        /* 文本区域特定样式 */
        textarea {
            min-height: 100px;
            resize: vertical;
            line-height: 1.5;
        }

        /* placeholder样式 */
        ::placeholder {
            color: #95a5a6;
            opacity: 1;
        }

        /* 悬停状态增强 */
        input[type="date"]:hover,
        textarea:hover {
            border-color: #bdc3c7;
        }

        button {
            /* 布局 */
            width: 100%;
            padding: 12px; 

            /* 视觉 */
            background: var(--secondary-color);
            color: white;
            border: none;
            border-radius: 4px;

            /* 交互 */
            cursor: pointer;
            transition: opacity 0.3s;
        }

        .analysis-report {
            max-width: 1200px;
            margin: 20px auto;
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        }

        .progress-info {
            margin: 10px 0;
            padding: 12px;
            background: #f8f9fa;
            border: 1px solid #eee;
            border-radius: 6px;
            color: var(--primary-color);
            font-size: 14px;
            transition: all 0.3s ease;
            min-height: 20px;
        }

        .progress-info.highlight {
            background: #e3f2fd;
            border-color: #3498db;
        }

        .report-content {
            height: 500px;
            overflow-y: auto;
            padding: 10px;
            border: 1px solid #eee;
            border-radius: 6px;
            margin-top: 15px;
        }

        .report-content .markdown-body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
            line-height: 1.6;
            color: var(--primary-color);
        }

        .report-content h1,
        .report-content h2,
        .report-content h3 {
            color: var(--primary-color);
            border-bottom: 1px solid #eee;
            padding-bottom: 0.3em;
        }

        .report-content pre {
            background: #f6f8fa;
            padding: 16px;
            border-radius: 6px;
            overflow: auto;
        }

        .report-content table {
            border-collapse: collapse;
            width: 100%;
        }

        .report-content td,
        .report-content th {
            border: 1px solid #dfe2e5;
            padding: 6px 13px;
        }
    </style>
</head>

<body>
    <div class="container">
        <!-- 左侧控制区 -->
        <div class="control-panel">
            <h2>参数设置</h2>
            <div class="date-picker">
                <label>开始日期</label>
                <input type="date" id="startDate">
                <label>结束日期</label>
                <input type="date" id="endDate">
            </div>
            <div>
                <label>您的观点</label>
                <textarea id="userOpinion" rows="5" placeholder="输入您的初步分析观点..."></textarea>
            </div>
            <button onclick="get_article()">获取文章</button>
            <button onclick="handleStart()" style="margin-top: 10px;">AI分析</button>
        </div>

        <!-- 新闻列表区 -->
        <div class="news-list">
            <h2>舆情文章</h2>
            <div id="newsContainer"></div>
        </div>

        </div> <!-- 关闭原有的container -->
        <div class="analysis-report">
            <h2>分析报告</h2>
            <div id="progress"></div>
            <div id="analysisContent" class="report-content"></div>
        </div>
    </div>

    <script>
        // 在全局保存当前文章数据
        let currentArticles = [];
        // 在全局保存控制器
        let abortController = null;
        
        // 获取文章
        async function get_article() {
            const payload = {
                startDate: document.getElementById('startDate').value,
                endDate: document.getElementById('endDate').value,
                userOpinion: document.getElementById('userOpinion').value
            };

            try {
                const response = await fetch('/get_article', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify(payload)
                });
                
                const result = await response.json();
                if (result.status === 'success') {
                    renderNews(result.data);
                }
                
            } catch (error) {
                console.error('请求失败:', error);
            }
        }

        // 展示文章
        function renderNews(articles) {
            currentArticles = articles; // 保存文章数据
            const container = document.getElementById('newsContainer');
            container.innerHTML = ''; // 清空现有内容

            articles.forEach(article => {
                const newsItem = document.createElement('div');
                newsItem.className = 'news-item';
                
                // 新闻标题头
                const header = document.createElement('div');
                header.className = 'news-header';
                header.innerHTML = `
                    <div style="flex-grow: 1">${article.title}</div>
                `;

                // 新闻内容区
                const content = document.createElement('div');
                content.className = 'news-content';
                content.innerHTML = `
                    <div class="markdown-body">
                        ${DOMPurify.sanitize(marked.parse(article.content))}
                    </div>
                `;

                // 点击交互
                header.addEventListener('click', () => {
                    // 关闭其他所有新闻项
                    document.querySelectorAll('.news-item').forEach(item => {
                        if (item !== newsItem) {
                            item.classList.remove('active');
                            item.querySelector('.news-content').classList.remove('active');
                        }
                    });
                    
                    // 切换当前项状态
                    newsItem.classList.toggle('active');
                    content.classList.toggle('active');
                });

                newsItem.append(header, content);
                container.appendChild(newsItem);
            });
        }


        // AI 分析
        let isProcessing = false;
        let checkInterval = null;
        const progress = document.getElementById('progress');

        async function handleStart() {
            if (isProcessing) return;
            
            isProcessing = true;
            progress.innerHTML = '正在初始化...';
            
            try {
                const res = await fetch('/start', {method: 'POST'});
                const data = await res.json();
                startCheckingProgress(data.total);
            } catch (error) {
                console.error('启动失败:', error);
                isProcessing = false;
            }
        }

        function startCheckingProgress(total) {
            checkInterval = setInterval(async () => {
                try {
                    const res = await fetch('/get_progress');
                    const data = await res.json();
                    
                    progress.innerHTML = `处理进度: ${data.current}/${total}`;
                    
                    if (data.row) {
                        output.innerHTML = 
                            `最新处理文章：
ID: ${data.row.id}
标题: ${data.row.title}
内容: ${data.row.content}
时间: ${data.row.time}\n` + output.innerHTML;
                    }

                    if (data.current >= total) {
                        clearInterval(checkInterval);
                        isProcessing = false;
                        progress.innerHTML += ' - 处理完成！';
                    }
                } catch (error) {
                    console.error('获取进度失败:', error);
                }
            }, 500);
        }

        // 初始化日期
        document.getElementById('startDate').value =
            new Date(Date.now() - 7 * 86400000).toISOString().split('T')[0];
        document.getElementById('endDate').value =
            new Date().toISOString().split('T')[0];

        initNews();
    </script>
    <!-- Markdown 解析库 -->
    <script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/dompurify/3.0.5/purify.min.js"></script>
</body>

</html>



后端代码：

import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from flask import Flask, jsonify, request
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


@app.route('/start', methods=['POST'])
def start_process():
    app.logger.debug("收到 AI 分析请求")
    global current_index
    
    data = request.json
    articles = data.get('articles', [])
    user_opinion = data.get('userOpinion', '')
    
    if not articles:
        return json.dumps({'status': 'error', 'message': '没有可分析的文章数据'})

    with data_lock:
        current_index = 0

    def process():
        global current_index
        while True:
            with data_lock:
                if stop_event.is_set() or current_index >= len(data):
                    break
                row = data[current_index].to_dict()
                current_index += 1
    
    Thread(target=process).start()
    return jsonify({'status': 'started', 'total': len(data)})

@app.route('/get_progress', methods=['GET'])
def get_progress():
    with data_lock:
        return jsonify({
            'current': current_index,
            'total': len(df),
            'row': df.iloc[current_index-1].to_dict() if current_index > 0 else None
        })

if __name__ == '__main__':
    app.run(debug=True)


优化上述前端代码和后端代码，使点击“AI分析“按钮后功能能实现