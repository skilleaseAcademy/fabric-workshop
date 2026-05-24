-- Auto Generated (Do not modify) E21F9A66126F4FEC07A9E479F101EC5659ADEC4D3A5BDD9C6CC9E4369A920E86

CREATE   VIEW vw_kpi_revenue_yesterday_vs_ly AS
WITH max_date_cte AS (
    SELECT MAX(order_date_key) AS max_date
    FROM   fact_sales
    WHERE  is_revenue_recognised = 1
),
yesterday_revenue AS (
    SELECT
        SUM(f.revenue) AS revenue_yesterday
    FROM   fact_sales f
    CROSS JOIN max_date_cte m
    WHERE  f.order_date_key = DATEADD(day, -1, m.max_date)
      AND  f.is_revenue_recognised = 1
),
ly_same_day_revenue AS (
    SELECT
        SUM(f.revenue) AS revenue_same_day_last_year
    FROM   fact_sales f
    CROSS JOIN max_date_cte m
    WHERE  f.order_date_key = DATEADD(year, -1, DATEADD(day, -1, m.max_date))
      AND  f.is_revenue_recognised = 1
)
SELECT
    y.revenue_yesterday,
    l.revenue_same_day_last_year,
    y.revenue_yesterday - l.revenue_same_day_last_year                    AS variance_brl,
    CASE WHEN l.revenue_same_day_last_year > 0
         THEN 100.0 * (y.revenue_yesterday - l.revenue_same_day_last_year)
              / l.revenue_same_day_last_year
         ELSE NULL END                                                    AS variance_pct
FROM yesterday_revenue y CROSS JOIN ly_same_day_revenue l;