import os
from flask import Blueprint, jsonify, request, Response
from pathlib import Path
from Base import DBFile, LoggerController
from WebServer.app.services.future import FutureService
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent

api_bp = Blueprint('api', __name__)

logger = LoggerController(
    name="webserver",
    log_level="INFO",
    console_output=False,
    file_output=True,
    log_file="Web_Server.log",
    when='D'
)

analysis_service = FutureService(logger)

@api_bp.route('/get_klines', methods=['POST'])
def get_klines():
    data = request.json
    date = data.get('date', '')
    # 验证日期格式
    if isinstance(date, str):
        date = date if '-' not in date else date.replace('-', '')
    if isinstance(date, datetime):
        date = datetime.strftime(date, '%Y%m%d')
    parent_path = os.path.join(PROJECT_ROOT, 'WebServer', 'app', 'static', 'images')
    filename = f'{parent_path}/{date}_klines.html'

    if not filename:
        return jsonify({
            'status': 'error',
            'message': '无效的日期格式'
        })
        
    return analysis_service.get_klines(filename)

@api_bp.route('/get_reports', methods=['POST'])
def get_reports():
    data = request.json
    date = data.get('date', '')
    # 验证日期格式
    if isinstance(date, str):
        date = date if '-' not in date else date.replace('-', '')
    if isinstance(date, datetime):
        date = datetime.strftime(date, '%Y%m%d')
    parent_path = os.path.join(PROJECT_ROOT, 'WebServer', 'app', 'static', 'reports')
    filename = f'{parent_path}/{date}_report.markdown'
    if not filename:
        return jsonify({
            'status': 'error',
            'message': '无效的日期格式'
        })
        
    return analysis_service.get_reports(filename)