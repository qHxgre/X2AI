import os
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
from Base import BaseAI, DBFile, DBSQL, LoggerController, BaseBuilder
from AIBots.aibot_sentimental_sugar_manager.plotting import plt_klines_rank
from AIBots.aibot_sentimental_sugar_manager.templets import TEMPLET_INPUT_REPORT_DAILY, TEMPLET_INPUT_REPORT_ROLLING, TEMPLET_OUTPUT
from AIBots.aibot_sentimental_sugar_manager.schema import AIBotSentimentalSugarManagerSchema

pd.options.mode.chained_assignment = None  # 完全关闭警告

class AIBotSentimentalSugarManager(BaseAI, BaseBuilder):
    """舆情AI基金经理"""
    datasource_id = "aibot_sentimental_sugar_manager"
    unique_together = ["date", "method"]
    sort_by = [("date", "ascending"), ("method", "ascending")]
    indexes = ["date"]
    schema = AIBotSentimentalSugarManagerSchema

    def __init__(
        self,
        start_date: Optional[str]=None,
        end_date: Optional[str]=None,
        n_days: int=7,
        db: Optional[DBFile]=None,
        llms: str="deepseek",
        cache: bool=True,
    ) -> None:
        super().__init__()
        # 日志打印
        self.logger = LoggerController(
            name="sugar_manager",
            log_level="INFO",
            console_output=False,
            file_output=True,
            log_file="AIBot_sentimental_sugar_mamager.log",
            when='D'
        )

        # 文件路径
        self.parent_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))      # 项目路径
        self.filepath_prompt = os.path.join(self.parent_path, "AIBots", "aibot_sentimental_sugar_manager", "prompts")       # 系统提示词
        self.path_image = os.path.join(self.parent_path, 'WebServer', 'app', 'static', 'images')     # 图片存储路径
        self.path_markdown = os.path.join(self.parent_path, 'WebServer', 'app', 'static', 'reports')      # 报告存储路径

        # 获取提示词
        # -- 白糖期货研究体系
        self.prompt_suggar = self.read_md(os.path.join(self.filepath_prompt, "sugar_logic.md"))       # 白糖期货研究体系
        # -- 基础提示词
        prompt_base = self.read_md(os.path.join(self.filepath_prompt, "prompt_base.md"))
        self.prompt_base = prompt_base.format(sugar_logic=self.prompt_suggar)
        # -- 单日研究系统提示词
        prompt_daily = self.read_md(os.path.join(self.filepath_prompt, "prompt_daily.md"))
        self.sys_prompt_daily = prompt_daily.format(prompt_base=prompt_base)
        # -- 滚动多日研究系统提示词
        prompt_rolling = self.read_md(os.path.join(self.filepath_prompt, "prompt_rolling.md"))
        self.sys_prompt_rolling = prompt_rolling.format(prompt_base=prompt_base)

        # 初始化ai
        self.ai_llms = llms
        self.init_ai(llms_api=self.ai_llms)

        # 初始化数据库
        if db is None:
            self.handler = DBFile(
                base_path=os.path.join(self.parent_path, "DataBase"),
                log_name="dbfile_sugar",
                log_level="DEBUG"
            )
        else:
            self.handler = db
            
        # 日期范围
        date_format = "%Y-%m-%d"
        self.start_date = (datetime.now() - timedelta(days=n_days)).strftime(date_format) if start_date is None else start_date
        self.end_date = datetime.now().strftime(date_format) if end_date is None else end_date
        # 因为涉及到滚动窗口计算，因此多取10天的数据
        self.before_start_date = (datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=10)).strftime("%Y-%m-%d")

        # 初始化系统变量
        self.data = None
        self.reports = None
        self.static_simple = None
        self.static_group = None
        self.static_weighted = None
        self.static_timedecay = None
        self.result = {}

        # 初始化的日志
        self.logger.info(f"=====>>>>> 初始化开始 ")
        self.logger.info(f"表名: {self.datasource_id}, AI 模型: {llms}, 数据获取周期：{self.start_date} 至 {self.end_date}, 向前获取日期: {self.before_start_date}")
        self.logger.info(f"项目路径: {self.parent_path}")
        self.logger.info(f"图片存储路径: {self.path_image}")
        self.logger.info(f"报告存储路径: {self.path_markdown}")
        self.logger.info(f"=====<<<<< 初始化完毕")

        # 获取原属数据
        self.cache = cache
        if self.cache is True:
            try:
                self.raw_data = self.handler.read_dataframe(
                    table=self.datasource_id,
                    filters={'date': [self.before_start_date, self.end_date]}
                )
                self.raw_data['date'] = self.raw_data['date'].dt.strftime('%Y-%m-%d')
            except Exception as e:
                self.raw_data = None
                self.logger.warning("无法读取历史数据: {e}")

    def get_reports(self, table: str) -> pd.DataFrame:
        """获取研究报告"""
        data = self.handler.read_dataframe(
            table=table,
            filters={"date": [self.before_start_date, self.end_date]}
        )
        data["date"] = data["date"].dt.strftime("%Y-%m-%d")
        return data
        
    def filter_articles(self, data: pd.DataFrame) -> pd.DataFrame:
        """筛选文章数据"""
        # 剔除置信度低的分析报告
        data = data[data["confidence"]>0.5]
        # 按照日期和文章ID排序
        data = data.sort_values(["date", "title"], ascending=[True, True])
        return data

    def write_data(self, data: pd.DataFrame) -> None:
        """保存分析结构"""
        data["date"] = pd.to_datetime(data["date"])
        data[self.handler.DEFAULT_PARTITION_FIELD] = data["date"].dt.strftime("%Y%m")
        data = data.sort_values(self.unique_together).reset_index(drop=True)
        self.write(data)

    def hit_cache(self, today: str, method: str) -> bool:
        """获取已分析的文章数据数据"""
        if self.raw_data is None:
            return False
        dates = self.raw_data[self.raw_data['method']==method]['date'].unique().tolist()
        if today in dates:
            return True
        else:
            return False

    def save_reports(self, result: dict, method: str) -> None:
        """存储报告数据"""
        # 存储数据
        rows = []
        elements = {
            '全球供应': 'supply',
            '全球需求': 'demand',
            '能源政策与原油价格': 'energy',
            '国内因素': 'domestic',
            '市场宏观与情绪': 'market'
        }
        for date, values in result.items():
            row = {
                'date': date,
                'rating': values['rating'],
                'ranking': values['ranking'],
                'confidence': values['confidence'],
                'conclusion': values['conclusion']
            }
            for element, field in elements.items():
                row[f'bullish_{field}'] = values['bullish'][element] if element in values['bullish'] else ''
                row[f'bearish_{field}'] = values['bearish'][element] if element in values['bearish'] else ''
            rows.append(row)
        df = pd.DataFrame(rows)
        # 确定分析方式
        df['method'] = method
        # 确定AI模型
        df['llms'] = self.ai_llms
        normalized_df = self.normalize(df)
        normalized_df[self.handler.DEFAULT_PARTITION_FIELD] = normalized_df['date'].dt.strftime('%Y')
        self.write(normalized_df)

    def simple_static(self, data: pd.DataFrame) -> pd.DataFrame:
        """简单统计平均数、中位数、分位数"""
        def static(today: str, df: pd.DataFrame) -> pd.DataFrame:
            stats = df[['short_forecast', 'long_forecast', 'confidence']].agg(
                ['mean', 'median', lambda x: x.quantile(0.3), lambda x: x.quantile(0.5), lambda x: x.quantile(0.8)]
            ).T
            stats.columns = ['mean', 'median', 'quantile_30', 'quantile_50', 'quantile_80']
            stats = stats.reset_index().rename(columns={'index': 'type'})
            stats['date'] = today
            return stats

        static_df = (
            data.groupby("date")
            .apply(lambda g: static(g.name, g))
            .reset_index(drop=True)
        )
        cols = ['mean', 'median', 'quantile_30', 'quantile_50', 'quantile_80']
        static_df[cols] = static_df[cols].round(4)
        return static_df

    def group_static(self, data: pd.DataFrame) -> pd.DataFrame:
        """分组统计，包含置信度均值"""
        def static(today: str, df: pd.DataFrame) -> pd.DataFrame:
            def _group_static(series: pd.Series, conf: pd.Series) -> dict:
                bullish = series > 0
                bearish = series < 0
                total = len(series)
                return {
                    'bullish_count': int(bullish.sum()),
                    'bearish_count': int(bearish.sum()),
                    'bullish_ratio': round(bullish.sum() / total if total else None, 4),
                    'bearish_ratio': round(bearish.sum() / total if total else None, 4),
                    'bullish_mean': round(series[bullish].mean(), 4),
                    'bearish_mean': round(series[bearish].mean(), 4),
                    'bullish_std': round(series[bullish].std(), 4),
                    'bearish_std': round(series[bearish].std(), 4),
                    'bullish_conf_mean': round(conf[bullish].mean(), 4),
                    'bearish_conf_mean': round(conf[bearish].mean(), 4)
                }
            records = []
            for field in ['short_forecast', 'long_forecast']:
                stats = _group_static(df[field], df['confidence'])
                stats['type'] = field
                records.append(stats)
            result = pd.DataFrame(records)
            result["date"] = today
            return result

        static_df = (
            data.groupby("date")
            .apply(lambda g: static(g.name, g))
            .reset_index(drop=True)
        )
        return static_df

    def simple_weighted(self, data: pd.DataFrame) -> pd.DataFrame:
        """加权平均"""
        df = data.dropna()
        df["short_forecast_weighted"] = df["short_forecast"] * df["confidence"]
        df["long_forecast_weighted"] = df["long_forecast"] * df["confidence"]
        weighted_forecast = df.groupby("date", as_index=False)[["short_forecast_weighted", "long_forecast_weighted"]].mean()
        return weighted_forecast

    def timedecay_weighted(self, data: pd.DataFrame, window: int=5, decay_rate: float=0.5) -> pd.DataFrame:
        """时间衰减加权平均"""
        df = data.sort_values('date')
        df['date'] = pd.to_datetime(df['date'])
        result = {}
        for i in range(window, df.shape[0]+1):
            recent = df.iloc[i-window: i, :]
            max_date = recent['date'].max()
            recent['days_passed'] = (max_date - recent['date']).dt.days
            # 计算权重
            recent['weight'] = np.exp(-decay_rate * recent['days_passed'])
            # 归一化权重
            recent['weight'] = recent['weight'] / recent['weight'].sum()
            result[max_date] = {
                "short_forecast_timeweighted": (recent["short_forecast_weighted"] * recent["weight"]).sum(),
                "long_forecast_timeweighted": (recent["long_forecast_weighted"] * recent["weight"]).sum()
            }
        result = pd.DataFrame(result).T.reset_index().rename(columns={"index": "date"})
        return result

    def analyzing_daily(self, date_series: list, reports_list: pd.DataFrame) -> None:
        """分析当日报告"""
        def _analyzing(system_prompt: str, today_reports: pd.DataFrame):
            # 获取拼接用户提示词
            user_prompt = ""       # 报告列表
            for i in range(0, today_reports.shape[0]):
                row = today_reports.iloc[i, :]
                input_report = TEMPLET_INPUT_REPORT_DAILY.format(
                    i=i+1,
                    title=row["title"],
                    short_forecast=row["short_forecast"],
                    long_forecast=row["long_forecast"],
                    confidence=row["confidence"],
                    summary=row["summary"],
                    opinion=row["opinion"]
                )
                user_prompt += input_report

            # AI分析
            try:
                answer = self.ai_api(user_prompt=user_prompt, system_prompt=system_prompt, json_output=True, model='deepseek-reasoner')
                result = json.loads(answer)
                return result, "success"
            except Exception as e:
                return None, e

        result = {}
        for today in date_series:
            today = today.strftime('%Y-%m-%d')
            now = datetime.now()
            if (self.cache is True) and (self.hit_cache(today, method='daily') is True):
                self.logger.info(f"[单日研究] 命中缓存: {today}")
                continue
            today_reports = reports_list[reports_list['date']==today]
            reports_size = today_reports.shape[0]
            if reports_size == 0:
                self.logger.warning(f"[单日研究] {today} 当日无新的舆情新闻报告：{reports_size}。跳过！")
                continue
        
            today_report, msg = _analyzing(
                system_prompt=self.sys_prompt_daily,
                today_reports=today_reports
            )

            if msg == 'success':
                self.logger.info(f'[单日研究] {today} AI 分析成功, 报告数量: {reports_size}, 耗时: {datetime.now() - now} ')
                result[today] = today_report
            else:
                self.logger.warning(f'[单日研究] {today} AI 分析失败, 失败原因: {msg}')
        
        # 存储数据
        self.save_reports(result, 'daily')

    def analyzing_rolling(self, date_series: list, daily_reports: pd.DataFrame, rolling_n: int=7) -> None:
        """分析多日数据"""
        def _analyzing(system_prompt: str, rolling_repots: pd.DataFrame):
            # 用户提示词
            user_prompt = ""
            emapping_reverse = {
                'supply': '全球供应',
                'demand': '全球需求',
                'energy': '能源政策与原油价格',
                'domestic': '国内因素',
                'market': '市场宏观与情绪'
            }
            for i in range(0, rolling_repots.shape[0]):
                row = rolling_repots.iloc[i, :]
                bullish_elements, bearish_elements = '', ''
                for k, v in row.items():
                    if v == '':
                        continue
                    if 'bullish' in k:
                        bullish_elements += f"\n  * {emapping_reverse[k.replace('bullish_', '')]}: {v}"
                    elif 'bearish' in k:
                        bearish_elements += f"\n  * {emapping_reverse[k.replace('bearish_', '')]}: {v}"
                daily_report = TEMPLET_INPUT_REPORT_ROLLING.format(
                    date=row['date'],
                    rating=row['rating'],
                    ranking=row['ranking'],
                    confidence=row['confidence'],
                    conclusion=row['conclusion'],
                    bullish=bullish_elements,
                    bearish=bearish_elements,
                )
                user_prompt += daily_report

            # AI分析
            try:
                answer = self.ai_api(user_prompt=user_prompt, system_prompt=system_prompt, json_output=True, model='deepseek-reasoner')
                result = json.loads(answer)
                return result, "success"
            except Exception as e:
                return None, e

        result = {}
        for today in date_series:
            now = datetime.now()
            sd = (today - timedelta(days=rolling_n)).strftime('%Y-%m-%d')
            today = today.strftime('%Y-%m-%d')
            # 命中缓存
            if (self.cache is True) and (self.hit_cache(today, method='rolling')):
                self.logger.info(f"[滚动分析] {today} 命中缓存")
                continue
            # 获取滚动日期的报告
            rolling_repots = daily_reports[(daily_reports['date']>=sd) & (daily_reports['date']<=today)]
            if rolling_repots.shape[0] < 5:
                self.logger.warning(f'[滚动分析] {today} 缺少5天的分析报告，无法滚动分析！跳过！')
                continue

            # AI 分析
            today_report, msg = _analyzing(
                system_prompt=self.sys_prompt_rolling,
                rolling_repots=rolling_repots
            )
            if msg == 'success':
                self.logger.info(f'[滚动分析] {today} AI 分析成功！报告数量: {rolling_repots.shape[0]}, 耗时: {datetime.now() - now}')
                result[today] = today_report
            else:
                self.logger.warning(f'[滚动分析] {today} AI 分析失败！耗时: {datetime.now() - now}, 失败原因: {msg}')

        # 存储数据
        self.save_reports(result, 'rolling')

    def analyzing_validate(self) -> None:
        """结合实际走势验证"""
        pass

    def generate_markdown(self, today: str, method: str='rolling') -> None:
        """生成markdown"""
        df = DBFile().read_dataframe(table='aibot_sentimental_sugar_manager', filters={'date': [today, today]})
        temp = df[df['method']==method]
        if temp.shape[0] == 0:
            self.logger.warning(f"[生成 markdown] 失败: {today}, 缺乏数据！")
            return
        row = temp.iloc[0]

        emapping_reverse = {
            'supply': '全球供应',
            'demand': '全球需求',
            'energy': '能源政策与原油价格',
            'domestic': '国内因素',
            'market': '市场宏观与情绪'
        }

        bullish_str, bearish_str = '', ''
        for k, v in row.items():
            if v == '':
                continue
            if 'bullish' in k:
                bullish_str += f"\n    * {emapping_reverse[k.replace('bullish_', '')]}: {v}"
            elif 'bearish' in k:
                bearish_str += f"\n    * {emapping_reverse[k.replace('bearish_', '')]}: {v}"


        content = TEMPLET_OUTPUT.format(
            today=row['date'].strftime('%Y-%m-%d'),
            rating=row['rating'],
            ranking=row['ranking'],
            confidence=row['confidence'],
            conclusion=row['conclusion'],
            bullish=bullish_str,
            bearish=bearish_str
        )

        filepath_markdown = f"{self.path_markdown}/{today.replace('-', '')}_report.markdown"
        with open(filepath_markdown, 'w') as file:
            file.write(content)

    def plotting_analyzing(self, today: str, method: str='rolling') -> None:
        """画图分析"""
        start_date = (datetime.strptime(today, "%Y-%m-%d") - timedelta(days=180)).strftime("%Y-%m-%d")
        end_date = today

        # 获取行情数据
        future_bar1d = DBFile().read_dataframe(
            table='future_bar1d',
            filters={"date": [start_date, end_date]},
            columns=["date", "instrument", "high", "open", "low", "close"]
        )

        # 获取 AI 预测数据
        aibot_suggar = DBFile().read_dataframe(
            table=self.datasource_id,
            filters={"date": [start_date, end_date]}
        )
        aibot_suggar = aibot_suggar[aibot_suggar['method']==method]

        # 画出k线图和预测值的走势
        plt_df = pd.merge(future_bar1d, aibot_suggar[['date', 'ranking']].rename(columns={'ranking': 'score'}), how="outer", on=["date"])
        plt_df['date'] = plt_df['date'].dt.strftime('%Y-%m-%d')
        plt_df = plt_df.sort_values('date')
        filepath_image = f"{self.path_image}/{today.replace('-', '')}_klines.html"
        plt_klines_rank(data=plt_df, filepath=filepath_image)


    def ai_analyzing(self, start_date: Optional[str]=None, end_date: Optional[str]=None) -> None:
        """AI 分析
        # STEP 1: 单日AI分析
        # STEP 2: 滚动多日AI分析
        # STEP 3: 结合实际走势验证
        """
        start_date = self.start_date if start_date is None else start_date
        end_date = self.end_date if end_date is None else end_date
        before_start_date = (datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=20)).strftime("%Y-%m-%d")
        self.logger.info(f"开始 AI 分析, 开始日期: {start_date}, 结束日期: {end_date}, 向前获取日期: {before_start_date}")

        # STEP 1: 单日AI分析
        self.analyzing_daily(
            date_series=pd.date_range(start=start_date, end=end_date),
            reports_list=self.reports[(self.reports['date']>=start_date) & (self.reports['date']<=end_date)]
        )

        # STEP 2: 滚动多日AI分析
        daily_reports = self.handler.read_dataframe(
            table=self.datasource_id,
            filters={
                'date': [before_start_date, end_date],
                'method': ['daily']
        })
        daily_reports['date'] = daily_reports['date'].dt.strftime('%Y-%m-%d')
        daily_reports = daily_reports.sort_values('date').reset_index(drop=True)
        self.analyzing_rolling(
            date_series=pd.date_range(start=start_date, end=end_date),
            daily_reports=daily_reports
        )

        # STEP 3: 结合实际走势验证
        # self.analyzing_validate()

    def analyzing(self):
        """分析主函数"""
        # STEP 1: 获取数据
        t0 = datetime.now()
        self.data = self.get_reports(table="aibot_sentimental_sugar_researcher")
        t1 = datetime.now()
        if self.data.shape[0] > 0:
            self.logger.info(f"数据获取成功: {self.data.shape}, 耗时: {t1-t0}")
        else:
            self.logger.warning(f"数据获取失败: {self.data.shape}, 请检查！")
            return

        # STEP 2: 数据清洗
        self.reports = self.filter_articles(self.data)
        t2 = datetime.now()
        if len(self.reports) == 0:
            self.logger.warning(f"没有可分析的文章: {len(self.reports)} !")
            return
        self.logger.info(f"数据清洗, 待分析的文章数量: {len(self.reports)}, 耗时: {t2-t1}")

        # STEP 3: 分析数据 & 画图
        self.static_simple = self.simple_static(self.reports.copy())
        self.static_group = self.group_static(self.reports.copy())
        self.static_weighted = self.simple_weighted(self.reports.copy())
        self.static_timedecay = self.timedecay_weighted(self.static_weighted.copy())

        # STEP 4: AI 分析
        self.ai_analyzing()

        # STEP 5: 生成报告 & 生成图片
        self.generate_markdown(self.end_date)
        self.plotting_analyzing(self.end_date)
        