import pandas as pd
from pyecharts.charts import Kline, Bar, Grid
from pyecharts import options as opts


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