import os
import re
from datetime import datetime
from flask import jsonify
from Base import LoggerController

class FutureService:
    def __init__(self, logger: LoggerController):
        self.logger = logger
        self.logger.info("FutureService initialized")
    
    def get_klines(self, filepath: str):
        """获取指定目录文件下，指定日期的k线图"""
        try:
            if not os.path.exists(filepath):
                return jsonify({
                    'status': 'error',
                    'message': 'K线数据不存在'
                })
            
            with open(filepath, 'r', encoding='utf-8') as f:
                html_content = f.read()
            return jsonify({
                'status': 'success',
                'data': html_content
            })
        except Exception as e:
            self.logger.error(f"Error getting klines: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': '获取K线数据失败'
            })

    def get_reports(self, filepath: str):
        """获取指定目录文件下，指定日期的研究报告"""
        try:
            if not os.path.exists(filepath):
                return jsonify({
                    'status': 'error',
                    'message': '研究报告不存在'
                })
                
            with open(filepath, 'r', encoding='utf-8') as file:
                content = file.read()
            return jsonify({
                'status': 'success',
                'data': content
            })
        except Exception as e:
            self.logger.error(f"Error getting reports: {str(e)}")
            return jsonify({
                'status': 'error',
                'message': '获取研究报告失败'
            })