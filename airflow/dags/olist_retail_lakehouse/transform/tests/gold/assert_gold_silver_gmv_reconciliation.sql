-- tests/gold/assert_gold_silver_gmv_reconciliation.sql
-- Cross-layer check: tổng GMV ở Gold không được lệch Silver quá 0.01%
-- Test trả về 1 row nếu vi phạm, 0 rows nếu pass

WITH gold_total AS (
    SELECT SUM(gross_merchandise_value) AS gold_gmv
    FROM {{ ref('gld_sales_summary') }}
),

silver_total AS (
    SELECT SUM(oi.price + oi.freight_value) AS silver_gmv
    FROM {{ ref('slv_order_items') }} oi
    INNER JOIN {{ ref('slv_orders') }} o ON oi.order_id = o.order_id
    WHERE o.order_status = 'delivered'
),

diff AS (
    SELECT
        g.gold_gmv,
        s.silver_gmv,
        ABS(g.gold_gmv - s.silver_gmv)                                 AS abs_diff,
        ABS(g.gold_gmv - s.silver_gmv) / NULLIF(s.silver_gmv, 0) * 100 AS diff_pct
    FROM gold_total g, silver_total s
)

SELECT *
FROM diff
WHERE diff_pct > 0.01  -- threshold: 0.01%
