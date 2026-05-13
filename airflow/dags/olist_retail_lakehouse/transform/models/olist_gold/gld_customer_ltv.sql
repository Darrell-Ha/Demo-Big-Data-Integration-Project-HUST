-- models/olist_gold/gld_customer_ltv.sql

{{
    config(
        unique_key = 'customer_id',
        properties = {
            'location': "'" ~ var('gold_location') ~ "/gld_customer_ltv'",
            'sorted_by': "ARRAY['customer_id']"
        },
        merge_update_columns = [
            'customer_state', 'total_orders', 'lifetime_value',
            'avg_order_value', 'first_order_date', 'last_order_date',
            'customer_lifespan_days', 'ltv_tier', '_gold_processed_at'
        ],
        post_hook    = ["{{ tag_snapshot(this) }}"],
        tags = ['olist_gold', 'gld_customer_ltv']
    )
}}

WITH customers AS (
    SELECT customer_id, customer_unique_id, customer_state
    FROM {{ ref('slv_customers') }}
),

orders AS (
    SELECT order_id, customer_id, order_purchase_date
    FROM {{ ref('slv_orders') }}
    WHERE order_status = 'delivered'
),

payments AS (
    SELECT order_id, SUM(payment_value) AS order_total
    FROM {{ ref('slv_order_payments') }}
    GROUP BY order_id
),

base AS (
    SELECT
        c.customer_id,
        c.customer_state,
        COUNT(DISTINCT o.order_id)                   AS total_orders,
        SUM(p.order_total)                           AS lifetime_value,
        AVG(p.order_total)                           AS avg_order_value,
        MIN(CAST(o.order_purchase_date AS DATE))     AS first_order_date,
        MAX(CAST(o.order_purchase_date AS DATE))     AS last_order_date,
        -- Trino: date_diff(unit, start, end)
        date_diff(
            'day',
            MIN(CAST(o.order_purchase_date AS DATE)),
            MAX(CAST(o.order_purchase_date AS DATE))
        )                                            AS customer_lifespan_days
    FROM customers c
    INNER JOIN orders   o ON c.customer_id = o.customer_id
    INNER JOIN payments p ON o.order_id    = p.order_id
    GROUP BY 1, 2
),

-- Tính ngưỡng phân tier trong CTE riêng
-- Trino: approx_percentile chỉ dùng được dạng aggregate, không dùng được dạng OVER()
-- Nên tính global threshold trước rồi cross join vào
ltv_thresholds AS (
    SELECT
        approx_percentile(lifetime_value, 0.5) AS p50,
        approx_percentile(lifetime_value, 0.9) AS p90
    FROM base
),

tiered AS (
    SELECT
        b.*,
        CASE
            WHEN b.lifetime_value >= t.p90 THEN 'high'
            WHEN b.lifetime_value >= t.p50 THEN 'mid'
            ELSE 'low'
        END AS ltv_tier
    FROM base b
    CROSS JOIN ltv_thresholds t
),

new AS (
    SELECT
        customer_id,
        customer_state,
        total_orders,
        CAST(lifetime_value  AS DECIMAL(12,2)) AS lifetime_value,
        CAST(avg_order_value AS DECIMAL(10,2)) AS avg_order_value,
        first_order_date,
        last_order_date,
        customer_lifespan_days,
        ltv_tier,
        current_timestamp                      AS _gold_processed_at
    FROM tiered
)

SELECT new.*
FROM new

{% if is_incremental() %}
    {{ anti_join_incremental(this, ['customer_id']) }}
{% endif %}
