-- models/olist_gold/gld_delivery_performance.sql
-- On-time rate, avg delay theo customer_state theo tháng.
-- Grain: 1 row / customer_state / year_month

{{
    config(
        unique_key   = ['customer_state', 'year_month'],
        properties = {
            'location': "'" ~ var('gold_location') ~ "/gld_delivery_performance'",
            'sorted_by': "ARRAY['customer_state', 'year_month']",
            'partitioning': "ARRAY['year_month']"
        },
        merge_update_columns = [
            'total_delivered_orders', 'on_time_orders', 'late_orders',
            'on_time_rate_pct', 'avg_delay_days', 'max_delay_days',
            'median_delay_days', '_gold_processed_at'
        ],
        post_hook    = ["{{ tag_snapshot(this) }}"],
        tags = ['olist_gold', 'gld_delivery_performance']
    )
}}

{{ validate_date_vars() }}

WITH orders AS (
    SELECT
        o.order_id,
        o.order_purchase_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        c.customer_state,

        -- Trino: format_datetime thay DATE_FORMAT
        format_datetime(CAST(o.order_purchase_date AS TIMESTAMP), 'yyyy-MM') AS year_month,

        -- Trino date_diff(unit, start, end) — start = estimated, end = actual
        -- Kết quả dương = trễ, âm = sớm hơn dự kiến
        date_diff(
            'day',
            CAST(o.order_estimated_delivery_date AS DATE),
            CAST(o.order_delivered_customer_date AS DATE)
        ) AS delivery_delay_days,

        CASE
            WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date
            THEN TRUE ELSE FALSE
        END AS is_on_time

    FROM {{ ref('slv_orders') }} o
    INNER JOIN {{ ref('slv_customers') }} c ON o.customer_id = c.customer_id
    WHERE o.order_status                    = 'delivered'
      AND o.order_delivered_customer_date   IS NOT NULL
      AND o.order_estimated_delivery_date   IS NOT NULL
      {% if is_incremental() %}
        AND {{ date_filter('o.order_purchase_date') }}
      {% endif %}
),

aggregated AS (
    SELECT
        customer_state,
        year_month,
        COUNT(*)                                        AS total_delivered_orders,
        SUM(CASE WHEN is_on_time     THEN 1 ELSE 0 END) AS on_time_orders,
        SUM(CASE WHEN NOT is_on_time THEN 1 ELSE 0 END) AS late_orders,
        AVG(CAST(delivery_delay_days AS DOUBLE))        AS avg_delay_days,
        MAX(delivery_delay_days)                        AS max_delay_days,
        -- Trino: approx_percentile thay PERCENTILE_CONT ... WITHIN GROUP
        approx_percentile(delivery_delay_days, 0.5)     AS median_delay_days
    FROM orders
    GROUP BY 1, 2
),

new AS (
    SELECT
        customer_state,
        year_month,
        total_delivered_orders,
        on_time_orders,
        late_orders,
        CAST(
            CAST(on_time_orders AS DOUBLE)
            / NULLIF(total_delivered_orders, 0) * 100
            AS DECIMAL(5,2)
        )                                               AS on_time_rate_pct,
        CAST(avg_delay_days    AS DECIMAL(6,1))         AS avg_delay_days,
        max_delay_days,
        CAST(median_delay_days AS DECIMAL(6,1))         AS median_delay_days,
        current_timestamp                               AS _gold_processed_at
    FROM aggregated
)

SELECT new.*
FROM new

{% if is_incremental() %}
    {{ anti_join_incremental(this, ['customer_state', 'year_month']) }}
{% endif %}
