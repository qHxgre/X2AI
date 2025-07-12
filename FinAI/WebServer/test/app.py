from flask import Flask, send_file, jsonify

app = Flask(__name__)

# 提供静态HTML文件
@app.route('/')
def index():
    return send_file('static/index.html')

if __name__ == '__main__':
    app.run(debug=True, port=5000)