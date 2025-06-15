from flask import Blueprint, jsonify, request, Response
from pathlib import Path
from datetime import datetime, timedelta
from Base import DBFile, LoggerController
from AIBots.SentimentalBot.robot import SentimentalBot
from AIBots.StockBot.robot import StockBot
from WebServer.app.services.analyzing import AnalysisService
from WebServer.app.services.reporting import ReportService
from WebServer.app.services.stockAnalyzer import stockService

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent

api_bp = Blueprint('api', __name__)

logger = LoggerController(
    name="webserver",
    log_level="INFO",
    console_output=False,
    file_output=True,
    log_file="WebServer.log",
    when='D'
)

aibot = SentimentalBot(db=DBFile())
stock_bot = StockBot(db=DBFile())

analysis_service = AnalysisService(aibot, logger)
report_service = ReportService(aibot, logger)
stock_service = stockService(aibot, logger)

@api_bp.route('/get_article', methods=['POST'])
def get_article():
    data = request.json
    start_date = data.get('startDate')
    end_date = data.get('endDate')
    return analysis_service.get_articles(start_date, end_date)

@api_bp.route('/get_chart')
def get_chart():
    return report_service.get_chart(f"{PROJECT_ROOT}/WebServer/app/static/images")

@api_bp.route('/get_reports')
def get_reports():
    return report_service.get_reports()

@api_bp.route('/analyze', methods=['POST'])
def analyze_articles():
    data = request.json
    articles = data.get('articles', [])
    user_opinion = data.get('userOpinion', '')
    return Response(analysis_service.analyze_articles(articles), mimetype='text/event-stream')

@api_bp.route('/analyzing_stocks')
def analyzing_stocks():
    results = stock_service.get_results()
    return jsonify(results)

@api_bp.route('/api/logging', methods=['POST'])
def log_message():
    data = request.get_json()
    message = data.get('message', '')
    level = data.get('level', '')
    
    if level == 'debug':
        logger.debug(message)
    elif level == 'info':
        logger.info(message)
    elif level == 'warning':
        logger.warning(message)
    elif level == 'error':
        logger.error(message)
    elif level == 'critical':
        logger.critical(message)
    else:
        raise ValueError("Invalid log level provided")