# 后端代码（Flask app.py）
from flask import Flask, jsonify, render_template
import pandas as pd
import time
from threading import Thread, Event, Lock

app = Flask(__name__)

# 生成虚构数据
def generate_data():
    data = {
        'id': [i for i in range(1, 6)],
        'title': [f'文章标题{i}' for i in range(1, 6)],
        'content': [f'这里是文章{i}的详细内容...' for i in range(1, 6)],
        'time': pd.date_range(start='2023-01-01', periods=5).strftime('%Y-%m-%d').tolist()
    }
    return pd.DataFrame(data)

# 共享数据
df = generate_data()
current_index = 0
stop_event = Event()
data_lock = Lock()

@app.route('/')
def index():
    return render_template('testindex.html')

@app.route('/start', methods=['POST'])
def start_process():
    global current_index
    with data_lock:
        current_index = 0
        stop_event.clear()
    
    def process():
        global current_index
        while True:
            with data_lock:
                if stop_event.is_set() or current_index >= len(df):
                    break
                row = df.iloc[current_index].to_dict()
                current_index += 1
            
            # 模拟处理延迟
            time.sleep(1)
    
    Thread(target=process).start()
    return jsonify({'status': 'started', 'total': len(df)})

@app.route('/get_progress', methods=['GET'])
def get_progress():
    with data_lock:
        return jsonify({
            'current': current_index,
            'total': len(df),
            'row': df.iloc[current_index-1].to_dict() if current_index > 0 else None
        })

@app.route('/stop', methods=['POST'])
def stop_process():
    with data_lock:
        stop_event.set()
    return jsonify({'status': 'stopped'})

if __name__ == '__main__':
    app.run(debug=True)