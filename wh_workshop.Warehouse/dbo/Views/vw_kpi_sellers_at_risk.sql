-- Auto Generated (Do not modify) 8B3385E3611B2E6FDD9D8734CF4BBBB874A70495F8ED8C27B9D759FFB358E166
CREATE   VIEW vw_kpi_sellers_at_risk AS
WITH max_date_cte AS (
    SELECT MAX(order_date_key) AS max_date FROM fact_sales
),
seller_breaches AS (
    SELECT
        f.seller_key,
        COUNT(*)                                  AS items_past_shipping_limit,
        SUM(f.revenue)                            AS revenue_at_risk,
        MIN(f.shipping_limit_date)                AS earliest_breach_date,
        MAX(DATEDIFF(day, f.shipping_limit_date, m.max_date))
                                                  AS max_days_overdue
    FROM   fact_sales f
    CROSS JOIN max_date_cte m
    WHERE  f.shipping_limit_date < m.max_date
      AND  f.order_delivered_carrier_date IS NULL
      AND  f.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY f.seller_key
)
SELECT
    s.seller_key,
    s.seller_city,
    s.seller_state,
    b.items_past_shipping_limit,
    b.revenue_at_risk,
    b.max_days_overdue,
    b.earliest_breach_date,
    -- Risk tier for the dashboard
    CASE
        WHEN b.max_days_overdue >= 14 THEN 'CRITICAL'
        WHEN b.max_days_overdue >=  7 THEN 'HIGH'
        WHEN b.max_days_overdue >=  3 THEN 'MEDIUM'
        ELSE                                'LOW'
    END                                            AS risk_tier
FROM   seller_breaches b
JOIN   dim_seller s ON s.seller_key = b.seller_key;