-- models/olist_gold/gld_category_revenue.sql
-- Doanh thu, số đơn, avg price theo category theo tháng.
-- Grain: 1 row / product_category_name_english / year_month

{{
    config(
        unique_key = ['product_category_name_english', 'year_month'],
        properties = {
            'location': "'" ~ var('gold_location') ~ "/gld_category_revenue'",
            'sorted_by': "ARRAY['product_category_name_english', 'year_month']"
        },
        merge_update_columns = [
            'total_orders', 'total_items_sold',
            'category_product_revenue', 'category_freight_revenue',
            'category_total_revenue', 'avg_product_price',
            'avg_review_score', '_gold_processed_at'
        ],
        post_hook    = ["{{ tag_snapshot(this) }}"],
        tags = ['olist_gold', 'gld_category_revenue']
    )
}}

WITH order_items AS (
    SELECT * FROM {{ ref('slv_order_items') }}
),

orders AS (
    SELECT order_id, order_purchase_date
    FROM {{ ref('slv_orders') }}
    WHERE order_status = 'delivered'
),

products AS (
    SELECT product_id, product_category_name_english
    FROM {{ ref('slv_products') }}
),

reviews AS (
    SELECT order_id, review_score
    FROM {{ ref('slv_order_reviews') }}
    WHERE is_score_out_of_range = FALSE
),

base AS (
    SELECT
        COALESCE(p.product_category_name_english, 'unknown')        AS product_category_name_english,
        -- Trino: format_datetime thay DATE_FORMAT
        format_datetime(CAST(o.order_purchase_date AS TIMESTAMP), 'yyyy-MM') AS year_month,
        COUNT(DISTINCT oi.order_id)                                 AS total_orders,
        COUNT(oi.order_item_id)                                     AS total_items_sold,
        SUM(oi.price)                                               AS category_product_revenue,
        SUM(oi.freight_value)                                       AS category_freight_revenue,
        SUM(oi.price + oi.freight_value)                            AS category_total_revenue,
        AVG(oi.price)                                               AS avg_product_price,
        AVG(r.review_score)                                         AS avg_review_score
    FROM order_items oi
    INNER JOIN orders   o ON oi.order_id  = o.order_id
    LEFT JOIN  products p ON oi.product_id = p.product_id
    LEFT JOIN  reviews  r ON oi.order_id  = r.order_id
    GROUP BY 1, 2
),

new AS (
    SELECT
        product_category_name_english,
        year_month,
        total_orders,
        total_items_sold,
        CAST(category_product_revenue  AS DECIMAL(15,2)) AS category_product_revenue,
        CAST(category_freight_revenue  AS DECIMAL(15,2)) AS category_freight_revenue,
        CAST(category_total_revenue    AS DECIMAL(15,2)) AS category_total_revenue,
        CAST(avg_product_price         AS DECIMAL(10,2)) AS avg_product_price,
        CAST(avg_review_score          AS DECIMAL(3,2))  AS avg_review_score,
        current_timestamp                                AS _gold_processed_at
    FROM base
)

SELECT new.*
FROM new

{% if is_incremental() %}
    {{ anti_join_incremental(this, ['product_category_name_english', 'year_month']) }}
{% endif %}
