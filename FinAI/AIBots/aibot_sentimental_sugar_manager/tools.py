import numpy as np
import pandas as pd
from pyecharts.charts import Kline, Bar, Grid
from pyecharts import options as opts



def simple_static(data: pd.DataFrame) -> pd.DataFrame:
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

def group_static(data: pd.DataFrame) -> pd.DataFrame:
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

def simple_weighted(data: pd.DataFrame) -> pd.DataFrame:
    """加权平均"""
    df = data.dropna()
    df["short_forecast_weighted"] = df["short_forecast"] * df["confidence"]
    df["long_forecast_weighted"] = df["long_forecast"] * df["confidence"]
    weighted_forecast = df.groupby("date", as_index=False)[["short_forecast_weighted", "long_forecast_weighted"]].mean()
    return weighted_forecast

def timedecay_weighted(data: pd.DataFrame, window: int=5, decay_rate: float=0.5) -> pd.DataFrame:
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

def plt_klines_rank(data: pd.DataFrame, filepath: str) -> None:
    """画出k线图和评级"""
    dates = data['date'].values.tolist()
    kline_data = data[['open', 'close', 'low', 'high']].values.tolist()
    rank_data = data["score"].values.tolist()

    # 创建K线图
    kline = (
        Kline()
        .add_xaxis(dates)
        .add_yaxis(
            "K线",
            kline_data,
            itemstyle_opts=opts.ItemStyleOpts(
                color="#ec0000",  # 上涨颜色
                color0="#00da3c",  # 下跌颜色
            ),
        )
        .set_global_opts(
            legend_opts=opts.LegendOpts(is_show=False),  # 隐藏图例
            xaxis_opts=opts.AxisOpts(
                type_="category",
                is_scale=True,
                boundary_gap=False,
                axisline_opts=opts.AxisLineOpts(is_on_zero=False),
                splitline_opts=opts.SplitLineOpts(is_show=False),
                split_number=20,
                min_="dataMin",
                max_="dataMax",
            ),
            yaxis_opts=opts.AxisOpts(
                is_scale=True,
                splitarea_opts=opts.SplitAreaOpts(
                    is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)
                ),
            ),
            tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="line"),
            datazoom_opts=[
                opts.DataZoomOpts(
                    is_show=False,
                    type_="inside",
                    xaxis_index=[0, 1],  # 同时控制两个图表的x轴
                    range_start=0,
                    range_end=100,
                ),
                opts.DataZoomOpts(
                    is_show=True,
                    xaxis_index=[0, 1],  # 同时控制两个图表的x轴
                    type_="slider",
                    pos_top="95%",
                    range_start=0,
                    range_end=100,
                ),
            ],
        )
    )

    # 创建柱状图
    bar = (
        Bar()
        .add_xaxis(dates)
        .add_yaxis(
            "AI评级",
            rank_data,
            yaxis_index=1,  # 使用第二个y轴
            label_opts=opts.LabelOpts(is_show=False),
            itemstyle_opts=opts.ItemStyleOpts(
                color=lambda params: "#ec0000" if params.value > 0 else "#00da3c"  # 根据值设置颜色
            ),
        )
        .set_global_opts(
            legend_opts=opts.LegendOpts(is_show=False),  # 隐藏图例
        )
    )


    # 使用Grid组合图表 - 移除固定尺寸，使用百分比布局
    grid = (
        Grid(init_opts=opts.InitOpts(width="100%", height="100%"))  # 使用百分比而非固定像素
        .add(
            kline,
            grid_opts=opts.GridOpts(
                pos_left="3%", pos_right="3%", pos_top="10%", height="60%"
            ),
        )
        .add(
            bar,
            grid_opts=opts.GridOpts(
                pos_left="3%", pos_right="3%", pos_top="75%", height="15%"
            ),
        )
    )

    # 添加响应式配置
    grid.add_js_funcs(
        """
        window.addEventListener('resize', function() {
            chart.resize();
        });
        """
    )


    # grid.render_notebook()
    grid.render(filepath)