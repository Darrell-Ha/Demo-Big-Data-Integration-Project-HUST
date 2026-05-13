-- models/olist_gold/gld_sales_summary.sql
-- GMV, order count, avg order value theo ngày.
-- Grain: 1 row / order_date

{{
    config(
        unique_key = 'order_date',
        properties = {
            'location': "'" ~ var('gold_location') ~ "/gld_sales_summary'",
            'sorted_by': "ARRAY['order_date']",
            'partitioning': "ARRAY['order_date']"
        },
        merge_update_columns = [
            'total_orders', 'orders_with_items', 'gross_merchandise_value',
            'product_revenue', 'freight_revenue', 'avg_order_value',
            'total_items_sold', 'avg_gmv_last_30d', 'is_revenue_spike',
            '_gold_processed_at'
        ],
        post_hook    = ["{{ tag_snapshot(this) }}"],
        tags = ['olist_gold', 'gld_sales_summary']
    )
}}

{{ validate_date_vars() }}

WITH delivered_orders AS (
    SELECT order_id, order_purchase_date
    FROM {{ ref('slv_orders') }}
    WHERE order_status = 'delivered'
    {% if is_incremental() %}
      AND {{ date_filter('order_purchase_date') }}
    {% endif %}
),

order_revenue AS (
    SELECT
        o.order_purchase_date                               AS order_date,
        COUNT(DISTINCT o.order_id)                          AS total_orders,
        COUNT(DISTINCT oi.order_id)                         AS orders_with_items,
        SUM(oi.price + oi.freight_value)                    AS gross_merchandise_value,
        SUM(oi.price)                                       AS product_revenue,
        SUM(oi.freight_value)                               AS freight_revenue,
        AVG(oi.price + oi.freight_value)                    AS avg_order_value,
        COUNT(oi.order_item_id)                             AS total_items_sold
    FROM delivered_orders o
    LEFT JOIN {{ ref('slv_order_items') }} oi ON o.order_id = oi.order_id
    GROUP BY 1
),

-- DQ: Spike detection — flag ngày có GMV vượt 3x avg 30 ngày trước
with_spike_flag AS (
    SELECT
        *,
        AVG(gross_merchandise_value) OVER (
            ORDER BY order_date
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS avg_gmv_last_30d,

        CASE
            WHEN gross_merchandise_value > (
                {{ var('dq_revenue_spike_multiplier') }}
                * AVG(gross_merchandise_value) OVER (
                    ORDER BY order_date
                    ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
                )
            )
            THEN TRUE ELSE FALSE
        END AS is_revenue_spike
    FROM order_revenue
),

new AS (
    SELECT
        order_date,
        total_orders,
        orders_with_items,
        CAST(gross_merchandise_value AS DECIMAL(15,2)) AS gross_merchandise_value,
        CAST(product_revenue         AS DECIMAL(15,2)) AS product_revenue,
        CAST(freight_revenue         AS DECIMAL(15,2)) AS freight_revenue,
        CAST(avg_order_value         AS DECIMAL(10,2)) AS avg_order_value,
        total_items_sold,
        CAST(avg_gmv_last_30d        AS DECIMAL(15,2)) AS avg_gmv_last_30d,
        is_revenue_spike,
        current_timestamp                              AS _gold_processed_at
    FROM with_spike_flag
)

SELECT new.*
FROM new

{% if is_incremental() %}
    {{ anti_join_incremental(this, ['order_date']) }}
{% endif %}
