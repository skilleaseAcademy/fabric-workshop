-- Auto Generated (Do not modify) D9C76F05817E1D83623A5E5020878301D39E8FAE5E116EBD751B38129E81E018
CREATE   VIEW vw_kpi_currently_overdue_orders AS

WITH max_date AS (
    SELECT MAX(order_date_key) AS max_order_date
    FROM fact_sales
)

SELECT
    COUNT(DISTINCT f.order_id) AS orders_currently_overdue,

    AVG(
        DATEDIFF(
            day,
            f.order_estimated_delivery_date,
            m.max_order_date
        )
    ) AS avg_days_overdue,

    -- Breakdown by status for dashboard
    SUM(CASE WHEN f.order_status = 'shipped'    THEN 1 ELSE 0 END) AS overdue_shipped,
    SUM(CASE WHEN f.order_status = 'invoiced'   THEN 1 ELSE 0 END) AS overdue_invoiced,
    SUM(CASE WHEN f.order_status = 'processing' THEN 1 ELSE 0 END) AS overdue_processing,
    SUM(CASE WHEN f.order_status = 'approved'   THEN 1 ELSE 0 END) AS overdue_approved

FROM fact_sales f
CROSS JOIN max_date m
WHERE f.is_currently_overdue = 1;