-- tests/silver/assert_orders_approval_completeness.sql
-- DQ Rule: >= 95% delivered/shipped orders phải có order_approved_at
-- Nếu tỷ lệ thiếu vượt threshold thì test này fail

WITH base AS (
    SELECT
        COUNT(*)                                            AS total_orders,
        SUM(CASE WHEN order_approved_at IS NULL THEN 1 ELSE 0 END)
                                                            AS missing_approval
    FROM {{ ref('slv_orders') }}
    WHERE order_status IN ('delivered','shipped','processing','invoiced')
),

check AS (
    SELECT
        total_orders,
        missing_approval,
        CAST(missing_approval AS DOUBLE) / NULLIF(total_orders, 0) AS missing_pct,
        1 - {{ var('dq_completeness_threshold') }}          AS allowed_missing_pct
    FROM base
)

-- Test fail nếu tỷ lệ thiếu vượt ngưỡng (trả về rows = số vi phạm)
SELECT *
FROM check
WHERE missing_pct > allowed_missing_pct
