import os
import re
from datetime import datetime
from flask import jsonify
from Base import LoggerController

class FutureService:
    def __init__(self, logger: LoggerController):
        self.logger = logger
        self.logger.info("FutureService initialized")
    
    def get_klines(self, directory: str):
        """获取指定目录文件下，指定日期的k线图"""
        try:
            # 获取目录下所有.html文件
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
            
            if not dated_files:
                self.logger.warning("没有找到任何图表文件!")
                return "No chart files found", 404
            
            # 按日期排序并返回最新的文件
            dated_files.sort(reverse=True)
            latest_file = dated_files[0][1]
            last_date = latest_file.replace(".html", "")
            self.logger.info(f"[自动分析] 找到 {len(dated_files)} 个图表文件, 最新日期: {last_date}")
            if last_date != datetime.now().strftime("%Y%m%d"):
                self.logger.warning(f"最新图表日期 {last_date} 与当前日期不一致!")

            filepath = os.path.join(directory, latest_file)
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