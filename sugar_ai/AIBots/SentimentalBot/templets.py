
TEMPLET_ARTICLE = """
* 文章标题: {title}
* 发布时间: {publish_date}
* 发布分类: {category}
* 子分类: {sub_category}
* 内容简介: {brief}
* 正文: {content}
"""

TEMPLET_ASSISTANT_REPORT = """
## 第 {i} 篇文章
* 文章标题: {title}
* 发布时间: {publish_date}
* 内容总结: {summary}
* 个人观点: {suggestion}
"""


TEMPLET_REPORT = """
分析日期: {analyze_date}
发布日期: {publish_date}

# 投资评级

{rating}

# 报告正文

## 摘要

{overview}

## 利好分析

{bullish}

## 利空分析

{bearish}

## 结论

{conclusion}

# 风险提示

{risk}
"""