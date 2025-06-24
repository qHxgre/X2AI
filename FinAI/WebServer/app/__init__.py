from flask import Flask
from WebServer.app.routes.views import views_bp
from WebServer.app.routes.api import api_bp

def create_app():
    app = Flask(__name__)
    
    # 初始化配置
    # from utils.config import Config
    # app.config.from_object(Config)
    
    # 注册蓝图
    # views_bp: 处理常规页面视图的蓝图
    # api_bp: 处理 API 请求的蓝图，所有路由都会以 /api 开头
    app.register_blueprint(views_bp)
    app.register_blueprint(api_bp, url_prefix='/api')
    
    return app