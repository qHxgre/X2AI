SQL_pe_ttm = """
WITH table_financial AS (
    WITH table_temp AS (
        SELECT
            date, instrument, shift, report_date,
            -- 扣非归母净利润
            net_profit_to_parent_shareholders_ttm - nonrecurring_income_to_owner_ttm as net_profit_to_parent_shareholders_deducted_ttm
        FROM cn_stock_financial_ttm_shift
        PRUNE JOIN cn_stock_financial_notes_shift USING (date, instrument, shift, report_date)
        QUALIFY shift=0
    )
    SELECT base.date, base.instrument, temp.net_profit_to_parent_shareholders_deducted_ttm,
    FROM cn_stock_instruments as base
    ASOF JOIN table_temp as temp
        ON base.instrument = temp.instrument
        AND temp.date <= base.date
)
SELECT
    date, instrument,
    total_shares / net_profit_to_parent_shareholders_deducted_ttm as pe_ttm
FROM table_financial
PRUNE JOIN cn_stock_shares USING (date, instrument)
"""


SQL_roe_ttm = """
WITH table_temp AS (
    SELECT
        date, instrument, shift, report_date,
        -- 扣非归母净利润
        net_profit_to_parent_shareholders_ttm - nonrecurring_income_to_owner_ttm as net_profit_to_parent_shareholders_deducted_ttm,
        -- 平均归母净资产
        LAG(total_equity_to_parent_shareholders_lf, 1) OVER (PARTITION BY date, instrument ORDER BY report_date ASC) AS total_equity_to_parent_shareholders_lf_shift_1,
        (total_equity_to_parent_shareholders_lf_shift_1 + total_equity_to_parent_shareholders_lf) / 2 as total_equity_to_parent_shareholders_lf_avg,
        -- roe_ttm
        net_profit_to_parent_shareholders_deducted_ttm / total_equity_to_parent_shareholders_lf_avg as roe_ttm,
    FROM cn_stock_financial_lf_shift
    PRUNE JOIN cn_stock_financial_ttm_shift USING (date, instrument, shift, report_date)
    PRUNE JOIN cn_stock_financial_notes_shift USING (date, instrument, shift, report_date)
    QUALIFY shift=0
)
SELECT base.date, base.instrument, temp.roe_ttm,
FROM cn_stock_instruments as base
ASOF JOIN table_temp as temp
    ON base.instrument = temp.instrument
    AND temp.date <= base.date
"""


SQL_net_profit_ttm_yoy = """
WITH table_temp AS (
    SELECT
        date, instrument, shift, report_date,
        -- 净利润增长率
        LAG(net_profit_ttm, 4) OVER (PARTITION BY date, instrument ORDER BY report_date ASC) AS _net_profit_ttm_shift_4,
        IF(_net_profit_ttm_shift_4 < 0, null, net_profit_ttm / _net_profit_ttm_shift_4 - 1) AS _net_profit_ttm_yoy,
        IF(isinf(_net_profit_ttm_yoy), NULL, _net_profit_ttm_yoy) AS net_profit_ttm_yoy,
    FROM cn_stock_financial_ttm_shift
    QUALIFY shift=0
)
SELECT base.date, base.instrument, temp.net_profit_ttm_yoy,
FROM cn_stock_instruments as base
ASOF JOIN table_temp as temp
    ON base.instrument = temp.instrument
    AND temp.date <= base.date
"""


SQL_net_profit_to_parent_ttm_yoy = """
WITH table_temp AS (
    SELECT
        date, instrument, shift, report_date,
        -- 净利润增长率
        LAG(net_profit_to_parent_shareholders_ttm, 4) OVER (PARTITION BY date, instrument ORDER BY report_date ASC) AS _net_profit_to_parent_shareholders_ttm_shift_4,
        IF(_net_profit_to_parent_shareholders_ttm_shift_4 < 0, null, net_profit_ttm / _net_profit_to_parent_shareholders_ttm_shift_4 - 1) AS _net_profit_to_parent_shareholders_ttm_yoy,
        IF(isinf(_net_profit_to_parent_shareholders_ttm_yoy), NULL, _net_profit_to_parent_shareholders_ttm_yoy) AS net_profit_to_parent_ttm_yoy,
    FROM cn_stock_financial_ttm_shift
    QUALIFY shift=0
)
SELECT base.date, base.instrument, temp.net_profit_to_parent_ttm_yoy,
FROM cn_stock_instruments as base
ASOF JOIN table_temp as temp
    ON base.instrument = temp.instrument
    AND temp.date <= base.date
"""
