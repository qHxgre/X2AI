import os
import pandas as pd
import datetime
from typing import List, Optional

class DataTransfer:
    def __init__(self, base_path: str):
        self.base_path = base_path
        self.category_structure = {
            "国内新闻": [
                '最新资讯', '糖料生产', '食糖生产', '糖厂开工', '食糖产销',
                '海关数据', '现货报价/进口成本', '食糖消费', '白糖期货/期权',
                '糖业政策', '蔗区气象', '副产品/替代品', '产业风采', '通知/公告', '糖业会议/培训',
            ],
            "国际新闻": [
                '最新资讯', '糖料生产', '食糖生产', '食糖贸易', '食糖消费',
                '供需分析', '期货市场', '糖业政策', '环球财经'
            ],
            "行业研究": [
                '热点研究', '日报', '周报', '月报', '国内市场', '国际市场',
                '调研报告', '宏观研究', '权威解读', '季报年报',
                '糖业论文', '糖业科普',
            ]
        }

    def save(self, df: pd.DataFrame) -> None:
        """保存DataFrame到指定目录结构"""
        self._validate_data(df)
        
        for (date_val, category, sub_category), group_df in df.groupby(['date', 'category', 'sub_category']):
            dir_path = os.path.join(self.base_path, str(date_val.strftime("%Y-%m-%d")), category, sub_category)
            os.makedirs(dir_path, exist_ok=True)
            
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = os.path.join(dir_path, f"data_{timestamp}.parquet")
            group_df.to_parquet(file_path)

    def _validate_data(self, df: pd.DataFrame) -> None:
        """验证数据格式和分类有效性"""
        required_columns = {'article_id', 'date', 'category', 'sub_category', 'title', 'brief', 'content'}
        if not required_columns.issubset(df.columns):
            missing = required_columns - set(df.columns)
            raise ValueError(f"缺少必要列: {missing}")

        # 验证分类有效性
        valid_categories = self.category_structure.keys()
        invalid_categories = df[~df['category'].isin(valid_categories)]
        if not invalid_categories.empty:
            raise ValueError(f"无效分类: {invalid_categories['category'].unique().tolist()}")

        # 验证子分类有效性
        for cat, valid_subs in self.category_structure.items():
            cat_data = df[df['category'] == cat]
            invalid_subs = cat_data[~cat_data['sub_category'].isin(valid_subs)]
            if not invalid_subs.empty:
                raise ValueError(f"分类'{cat}'下无效的子分类: {invalid_subs['sub_category'].unique().tolist()}")

    def load(self, 
            start_date: str,
            end_date: str,
            categories: Optional[List[str]] = None,
            sub_categories: Optional[List[str]] = None) -> pd.DataFrame:
        """根据条件加载数据"""
        start_date = pd.to_datetime(start_date).date() if start_date else None
        end_date = pd.to_datetime(end_date).date() if end_date else None
        
        collected_files = []
        
        # 遍历目录结构
        for root, dirs, files in os.walk(self.base_path):
            # 解析路径要素
            path_parts = os.path.relpath(root, self.base_path).split(os.sep)
            
            # 验证路径深度
            if len(path_parts) != 3 or path_parts[0] == '.':
                continue
                
            date_str, category, sub_category = path_parts
            
            # 日期过滤
            try:
                current_date = pd.to_datetime(date_str).date()
            except ValueError:
                continue
                
            if (start_date and current_date < start_date) or (end_date and current_date > end_date):
                continue
                
            # 分类过滤
            if categories and category not in categories:
                continue
                
            # 子分类过滤
            if sub_categories and sub_category not in sub_categories:
                continue
                
            # 收集文件
            for file in files:
                if file.endswith(".parquet"):
                    collected_files.append(os.path.join(root, file))
        
        # 读取并合并数据
        if collected_files:
            return pd.concat([pd.read_parquet(f) for f in collected_files], ignore_index=True)
        return pd.DataFrame()
