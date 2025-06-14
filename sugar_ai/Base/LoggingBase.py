import logging
import os
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from typing import Optional, Union


class LoggerController:
    """
    日志等级:
    * logging.DEBUG: 详细的调试信息，通常只在诊断问题时使用
    * logging.INFO: 确认程序按预期运行的一般信息
    * logging.WARNING: 表明有潜在问题或意外情况，但程序仍能正常运行
    * logging.ERROR: 由于更严重的问题，程序某些功能无法正常执行
    * logging.CRITICAL: 严重的错误，可能导致程序无法继续运行
    """
    
    def __init__(
        self,
        name: str = 'root',
        log_level: str = 'INFO',
        console_output: bool = True,
        file_output: bool = False,
        log_file: Optional[str] = None,
        error_file: Optional[str] = None,
        max_bytes: Optional[int] = None,
        backup_count: int = 5,
        when: Optional[str] = None,
        interval: int = 1,
        fmt: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt: str = '%Y-%m-%d %H:%M:%S'
    ):
        """
        初始化日志控制器
        
        :param name: 日志名称
        :param log_level: 日志级别 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        :param console_output: 是否输出到控制台
        :param file_output: 是否输出到文件
        :param log_file: 普通日志文件路径
        :param error_file: 错误日志文件路径(单独存放ERROR及以上级别)
        :param max_bytes: 日志文件最大字节数(用于轮转)
        :param backup_count: 保留的备份文件数量
        :param when: 时间轮转间隔单位 (S, M, H, D, midnight等)
        :param interval: 轮转间隔
        :param fmt: 日志格式
        :param datefmt: 日期格式
        """
        # 配置日志文件路径
        self.parent_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Logs")

        self.logger = logging.getLogger(name)
        self.logger.setLevel(log_level.upper())

        # 防止重复添加handler
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
        
        self.formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
        
        # 控制台输出
        if console_output:
            self._add_console_handler()
        
        # 文件输出
        if file_output:
            if log_file:
                self._add_file_handler(
                    log_file, 
                    max_bytes=max_bytes, 
                    backup_count=backup_count,
                    when=when,
                    interval=interval,
                    level=logging.DEBUG
                )
            
            if error_file:
                self._add_file_handler(
                    error_file,
                    max_bytes=max_bytes,
                    backup_count=backup_count,
                    when=when,
                    interval=interval,
                    level=logging.ERROR
                )
    
    def _add_console_handler(self):
        """添加控制台handler"""
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(self.formatter)
        self.logger.addHandler(console_handler)
    
    def _add_file_handler(
        self,
        filename: str,
        level: int = logging.DEBUG,
        max_bytes: Optional[int] = None,
        backup_count: int = 7,
        when: Optional[str] = 'D',
        interval: int = 1
    ):
        """添加文件handler，支持按大小或时间轮转

        默认为1天存1个日志，保留7天的备份
        when: 轮换时间单位，可以是:
            * 'S' - 秒
            * 'M' - 分钟
            * 'H' - 小时
            * 'D' - 天
            * 'W0'-'W6' - 每周(0=周一)
            * 'midnight' - 每天午夜
        """
        # 确保日志目录存在
        filepath = os.path.join(self.parent_path, filename)
        os.makedirs(self.parent_path, exist_ok=True)
        
        if when:
            # 时间轮转
            file_handler = TimedRotatingFileHandler(
                filename=filepath,
                when=when,
                interval=interval,
                backupCount=backup_count,
                encoding='utf-8'
            )
        elif max_bytes:
            # 大小轮转
            file_handler = RotatingFileHandler(
                filename=filepath,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding='utf-8'
            )
        else:
            # 普通文件handler
            file_handler = logging.FileHandler(
                filename=filepath,
                encoding='utf-8'
            )
        
        file_handler.setLevel(level)
        file_handler.setFormatter(self.formatter)
        self.logger.addHandler(file_handler)
    
    def debug(self, msg: str, *args, **kwargs):
        """记录DEBUG级别日志"""
        self.logger.debug(msg, *args, **kwargs)
    
    def info(self, msg: str, *args, **kwargs):
        """记录INFO级别日志"""
        self.logger.info(msg, *args, **kwargs)
    
    def warning(self, msg: str, *args, **kwargs):
        """记录WARNING级别日志"""
        self.logger.warning(msg, *args, **kwargs)
    
    def error(self, msg: str, *args, **kwargs):
        """记录ERROR级别日志"""
        self.logger.error(msg, *args, **kwargs)
    
    def critical(self, msg: str, *args, **kwargs):
        """记录CRITICAL级别日志"""
        self.logger.critical(msg, *args, **kwargs)
    
    def exception(self, msg: str, *args, **kwargs):
        """记录异常日志"""
        self.logger.exception(msg, *args, **kwargs)
    
    def set_level(self, level: str):
        """设置日志级别"""
        self.logger.setLevel(level.upper())
    
    def add_file_handler(
        self,
        filename: str,
        level: Union[str, int] = 'DEBUG',
        max_bytes: Optional[int] = None,
        backup_count: int = 5,
        when: Optional[str] = None,
        interval: int = 1
    ):
        """动态添加文件handler"""
        if isinstance(level, str):
            level = getattr(logging, level.upper())
        
        self._add_file_handler(
            filename=filename,
            level=level,
            max_bytes=max_bytes,
            backup_count=backup_count,
            when=when,
            interval=interval
        )