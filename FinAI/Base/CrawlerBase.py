import requests
from Base.DataBase import BaseBuilder

class BaseCrawler(BaseBuilder):
    """爬虫基础类"""
    def __init__(self) -> None:
        pass
    
    def request(self, url, params, headers):
        return requests.get(url, params=params, headers=headers)
    