TEMPLET_INPUT_REPORT_DAILY = """
## 第 {i} 篇文章
* 文章标题：{title}
* 短期预测（未来一周）：{short_forecast}
* 长期预测（未来一个月）：{long_forecast}
* 置信度：{confidence}
* 内容总结：{summary}
* 分析观点：{opinion}
"""

TEMPLET_USER_PROMPT_DAILY = """
# 简单统计

{simple_static}

# 分组统计

{group_static}

# 当日报告列表

{reports_list}

"""


TEMPLET_INPUT_REPORT_ROLLING = """
# 日期：{date}
* 投资评级：{rating}
* 预测分数：{ranking}
* 置信度：{confidence}
* 分析结论

{conclusion}

* 看多因素
{bullish}

* 看空因素
{bearish}

"""


TEMPLET_USER_PROMPT_ROLLING = """
# 时间衰减加权平均统计值

{timedecay_static}

# 每日报告列表

{reports_list}

"""


TEMPLET_OUTPUT = """
## 日期：{today}

### 投资评级
* 投资评级：{rating}
* 预测分数：{ranking}
* 置信度：{confidence}

### 分析总结

{conclusion}

### 利好分析
{bullish}

### 利空分析
{bearish}

"""