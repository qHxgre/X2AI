import time
import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from base import BaseCrawler
from DataBuilder.hisugar.schema import HisugarSchema
from base import DBFile, DBSQL

class HigSugarCrawler(BaseCrawler):
    '目标网址（泛糖科技）：https://www.hisugar.com/home/newListMore?parentId=57&level=3&childId=151&menuTap3'
    datasource_id = "aisugar_hisugar"
    unique_together = ["date", "article_id", "category", "sub_category", "title"]
    sort_by = [("date", "ascending"), ("article_id", "ascending")]
    indexes = ["date"]
    schema = HisugarSchema

    def __init__(self, start_date: str, end_date: str, db=None) -> None:
        # 开始时间和结束时间
        self.start_date, self.end_date = start_date, end_date
        # 数据库：默认为 PostgresSQL
        self.handler = DBSQL() if db is None else db
        # 原始数据
        self.raw_data = self.handler.read_data(table=self.datasource_id, start_date=self.start_date, end_date=self.end_date)

        # 增量更新时每页 10 条，获取历史数据时每页50条
        date_format = "%Y-%m-%d"
        date_delta = (datetime.strptime(self.end_date, date_format) - datetime.strptime(self.start_date, date_format)).days
        self.pageSize = 10 if date_delta <= 20 else 30
        print(f"初始化! 爬取周期: {self.start_date} 至 {self.end_date}, 每页大小: {self.pageSize}")

        # 文章分类
        self.article_categories = {
            "国内新闻": {
                'firstCategory': '49',
                'caName': '国内新闻',
                'referer': 'https://www.hisugar.com/home/newListMore?parentId=57&level=3&childId=542&menuTap3',
                'pageCurInfo': {
                    'sub_143': '最新资讯',
                    'sub_582': '糖料生产',
                    'sub_55': '食糖生产',
                    'sub_52': '糖厂开工',
                    'sub_71': '食糖产销',
                    'sub_51': '海关数据',
                    'sub_141': '现货报价/进口成本',
                    'sub_53': '食糖消费',
                    'sub_142': '白糖期货/期权',
                    'sub_54': '糖业政策',
                    'sub_78': '蔗区气象',
                    'sub_131': '副产品/替代品',
                    'sub_70': '产业风采',
                    'sub_128': '通知/公告',
                    'sub_620': '糖业会议/培训',
                }
            },
            "国际新闻": {
                'firstCategory': '39',
                'caName': '国际新闻',
                'referer': 'https://www.hisugar.com/home/newListMore?parentId=39&level=3&childId=144&menuTap1',
                'pageCurInfo': {
                    'sub_144': '最新资讯',
                    'sub_40': '糖料生产',
                    'sub_41': '食糖生产',
                    'sub_42': '食糖贸易',
                    'sub_45': '食糖消费',
                    'sub_44': '供需分析',
                    'sub_145': '期货市场',
                    'sub_146': '糖业政策',
                    'sub_43': '环球财经'
                }
            },
            '行业研究': {
                'firstCategory': '57',
                'caName': '行业研究',
                'referer': 'https://www.hisugar.com/home/newListMore?parentId=57&level=3&childId=151&menuTap3',
                'pageCurInfo': {
                    'sub_542': '热点研究',
                    'sub_151': '日报',
                    'sub_59': '周报',
                    'sub_152': '月报',
                    'sub_60': '国内市场',
                    'sub_155': '国际市场',
                    'sub_154': '调研报告',
                    'sub_159': '宏观研究',
                    'sub_158': '权威解读',
                    'sub_153': '季报年报',
                    'sub_156': '糖业论文',
                    'sub_157': '糖业科普',
                }
            }
        }

        # 文章列表的headers
        self.list_headers = {
            'Accept': 'text/html, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
            'Connection': 'keep-alive',
            'Referer': None,
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
            'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
        }

        # 文章内容的headers
        self.content_headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
        }

        self.skip_subcategory = False       # 跳过子类的爬取
        self.existing_article = []          # 已爬取过的文章

    def write_log(self, msg: str, isprint: bool=True) -> None:
        """日志打印"""
        if isprint is True:
            print(msg)

    def crawl_category(self, category: dict) -> list:
        """爬取某一大类的数据"""
        category_name = category['caName']
        for sub_info, sub_name in category['pageCurInfo'].items():
            # 获取原始数据中的已存在文章ID
            raw_data = self.raw_data[
                (self.raw_data["category"] == category_name)
                & (self.raw_data["sub_category"] == sub_name)
            ]
            self.existing_article = raw_data['article_id'].unique().tolist()
            # 爬取该大类的文章
            now = datetime.now()
            input_params = {
                'category': category['caName'],
                'sub_category': sub_name,
                'params': {
                    'sortType': '01',
                    'firstCategory': category['firstCategory'],
                    'caName': category['caName'],
                    'pageCurInfo': sub_info,
                    'pageNo': '0',
                    'pageSize': self.pageSize,
                },
            }
            headers = self.list_headers.copy()
            headers["Referer"] = category["referer"]
            data = self.crawl_page(headers, input_params)
            if data.shape[0] == 0:
                continue

            normalized_df = self.normalize(data)
            self.write(normalized_df)
            self.write_log(f"爬取完成: {category_name} - {sub_name}, 数据大小: {data.shape}, 耗时: {datetime.now() - now}", True)

    def crawl_page(self, hearders: dict, input_params: dict) -> pd.DataFrame:
        """循环遍历每一页爬取数据"""
        category = input_params['category']
        sub_category= input_params['sub_category']
        params= input_params['params']
        pageNo = 0
        self.skip_subcategory = False
        result = []

        while True:     # 循环爬取每一页
            time.sleep(random.uniform(0, 1))        # 随机睡眠一段时间，以免被封IP
            pageNo += 1
            params['pageNo'] = str(pageNo)          # 修改爬取的页面
            self.write_log(f"爬取 [{category} - {sub_category}]: 第 {pageNo} 页")
            response = self.request('https://www.hisugar.com/home/getQureyArticleList', params=params, headers=hearders)
            soup = BeautifulSoup(response.text, "lxml")
            # 获取所有文章的内容
            articles_list = soup.find_all('li')      # 找到所有<li>元素
            if len(articles_list) == 0:      # 若当页未找到内容则返回
                break
            # 爬取当前页面
            result += self.crawl_page_list(articles_list, category, sub_category)
            if self.skip_subcategory is True:        # 跳出循环
                break
        return pd.DataFrame(result)

    def crawl_page_list(self, articles_list: list, category: str, sub_category: str) -> list:
        """爬取当前页面的文章列表"""
        date = ""
        temp_result = []
        for i, content_li in enumerate(articles_list):
            time.sleep(random.uniform(0, 1))                # 随机睡眠一段时间，以免被封IP
            link = content_li.find('a', href=lambda href: href and href.startswith('/home/articleContent?id='))
            if link:
                article_id = link['href'].split('id=')[-1]                                      # 文章id
                date = content_li.find('dl', class_='day').find_all('dd')[0].text.strip()       # 文章日期
                title = content_li.find('h3').text.strip()                                      # 文章标题
                brief = content_li.find('p', class_='word').text.strip()                        # 文章简介
                if self.check_existing(article_id, self.existing_article, title) is True:
                    continue
                if self.check_before(date, title) is True:
                    self.skip_subcategory = True
                    break
                if self.check_included(date, title) is False:
                    continue
                # 爬取指定文章的内容
                content = self.crawl_content(self.content_headers, article_id)
                if content == '':
                    self.write_log(f"[WARNING] 当前文章无法爬取文字内容，跳过:  {title}")
                    continue
                temp_result.append({
                    'article_id': article_id,
                    'date' : date,
                    'title': title,
                    'brief': brief,
                    'category': category,
                    'sub_category': sub_category,
                    'content': content
                })
                self.write_log(f"成功爬取文章: {title}, 发布日期: {date}", self.pageSize == 10)
        print(f"截止日期: {date}")
        return temp_result

    def check_existing(self, article_id: str, article_list: list, title: str) -> bool:
        """如果文章ID已存在，则跳过"""
        if article_id in article_list:
            self.write_log(f"[WARNING] 当前文章已爬取, 跳过: {title} ", self.pageSize == 10)
            return True

    def check_included(self, date: str, title: str) -> bool:
        """判断该文章的时间是否在目标范围内"""
        if ((pd.to_datetime(self.start_date)-timedelta(days=3)) <= pd.to_datetime(date)) & (pd.to_datetime(date) <= (pd.to_datetime(self.end_date)+timedelta(days=3))):
            return True
        else:
            self.write_log(f"[WARNING] 当前文章发布时间不在指定时间范围内, 跳过: {title} - {date}", self.pageSize == 10)
            return False

    def check_before(self, date: str, title: str) -> bool:
        """判断该文章的时间是否早于开始时间"""
        if pd.to_datetime(date) < pd.to_datetime(self.start_date):
            self.write_log(f"[WARNING] 当前文章发布时间超过开始时间，退出: {title}, {date}", self.pageSize != 10)
            return True
        else:
            return False

    def crawl_content(self, headers: dict, article_id: str) -> str:
        params = {'id': article_id}
        response = self.request('https://www.hisugar.com/home/articleContent', params=params, headers=headers)
        soup = BeautifulSoup(response.text, "lxml")
        content_divs = soup.find_all("div", class_="content")
        content = "".join([div.get_text(strip=True) for div in content_divs])

        # 删除免责声明的部分
        if '（责任编辑：Lewis）' in content:
            content = content[:content.find('（责任编辑：Lewis）')]
        elif '免责声明' in content:
            content = content[:content.find('免责声明')]
        return content

    def crawl(self):
        self.crawl_category(self.article_categories["国内新闻"])
        self.crawl_category(self.article_categories["国际新闻"])
        self.crawl_category(self.article_categories["行业研究"])
