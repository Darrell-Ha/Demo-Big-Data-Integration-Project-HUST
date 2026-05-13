-- models/olist_gold/gld_seller_performance.sql
-- Revenue, order count, avg rating theo seller theo tháng.
-- Grain: 1 row / seller_id / year_month

{{
    config(
        unique_key = ['seller_id', 'year_month'],
        properties = {
            'location': "'" ~ var('gold_location') ~ "/gld_seller_performance'",
            'sorted_by': "ARRAY['seller_id', 'year_month']",
            'partitioning': "ARRAY['year_month']"
        },
        merge_update_columns = [
            'seller_state', 'seller_city', 'total_orders', 'total_items',
            'total_revenue', 'product_revenue', 'avg_order_value',
            'avg_review_score', 'total_reviews', '_gold_processed_at'
        ],
        post_hook    = ["{{ tag_snapshot(this) }}"],
        tags = ['olist_gold', 'gld_seller_performance']
    )
}}

{{ validate_date_vars() }}

WITH order_items AS (
    SELECT * FROM {{ ref('slv_order_items') }}
),

orders AS (
    SELECT order_id, order_purchase_date
    FROM {{ ref('slv_orders') }}
    WHERE order_status = 'delivered'
    {% if is_incremental() %}
      AND {{ date_filter('order_purchase_date') }}
    {% endif %}
),

reviews AS (
    SELECT order_id, review_score
    FROM {{ ref('slv_order_reviews') }}
    WHERE is_score_out_of_range = FALSE
),

sellers AS (
    SELECT seller_id, seller_state, seller_city
    FROM {{ ref('slv_sellers') }}
),

base AS (
    SELECT
        oi.seller_id,
        -- Trino: format_datetime thay cho DATE_FORMAT
        format_datetime(CAST(o.order_purchase_date AS TIMESTAMP), 'yyyy-MM') AS year_month,
        COUNT(DISTINCT oi.order_id)      AS total_orders,
        COUNT(oi.order_item_id)          AS total_items,
        SUM(oi.price + oi.freight_value) AS total_revenue,
        SUM(oi.price)                    AS product_revenue,
        AVG(oi.price + oi.freight_value) AS avg_order_value,
        AVG(r.review_score)              AS avg_review_score,
        COUNT(r.review_score)            AS total_reviews
    FROM order_items oi
    INNER JOIN orders  o ON oi.order_id = o.order_id
    LEFT JOIN  reviews r ON oi.order_id = r.order_id
    GROUP BY 1, 2
),

new AS (
    SELECT
        b.seller_id,
        b.year_month,
        s.seller_state,
        s.seller_city,
        b.total_orders,
        b.total_items,
        CAST(b.total_revenue    AS DECIMAL(15,2)) AS total_revenue,
        CAST(b.product_revenue  AS DECIMAL(15,2)) AS product_revenue,
        CAST(b.avg_order_value  AS DECIMAL(10,2)) AS avg_order_value,
        CAST(b.avg_review_score AS DECIMAL(3,2))  AS avg_review_score,
        b.total_reviews,
        current_timestamp                         AS _gold_processed_at
    FROM base b
    LEFT JOIN sellers s ON b.seller_id = s.seller_id
)

SELECT new.*
FROM new

{% if is_incremental() %}
    {{ anti_join_incremental(this, ['seller_id', 'year_month']) }}
{% endif %}
