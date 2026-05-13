-- models/olist_silver/slv_order_items.sql

{{
    config(
        unique_key = ['order_id', 'order_item_id'],
        properties = {
            'location': "'" ~ var('silver_location') ~ "/slv_order_items'",
            'sorted_by': "ARRAY['order_id', 'order_item_id']"
        },
        merge_update_columns = [
            'product_id', 'seller_id', 'shipping_limit_date',
            'shipping_limit_date_day', 'price', 'freight_value',
            'total_item_value', 'is_price_anomaly', 'is_freight_anomaly',
            '_silver_processed_at'
        ],
        post_hook = ["{{ tag_snapshot(this) }}"],
        tags = ['olist_silver', 'slv_order_items']
    )
}}

WITH src AS (
    SELECT * FROM {{ source('olist_bronze', 'order_items') }}
),

{{ dedup_cte('src', ['order_id', 'order_item_id']) }},

-- Referential integrity: chỉ giữ items có order tồn tại trong silver_orders
valid_orders AS (
    SELECT DISTINCT order_id FROM {{ ref('slv_orders') }}
),

cleaned AS (
    SELECT
        b.order_id,
        b.order_item_id,
        b.product_id,
        b.seller_id,
        CAST(b.shipping_limit_date AS TIMESTAMP)     AS shipping_limit_date,
        CAST(b.shipping_limit_date AS DATE)          AS shipping_limit_date_day,
        CAST(b.price         AS DECIMAL(10,2))       AS price,
        CAST(b.freight_value AS DECIMAL(10,2))       AS freight_value,
        CAST(b.price AS DECIMAL(10,2))
            + CAST(b.freight_value AS DECIMAL(10,2)) AS total_item_value,

        CASE WHEN CAST(b.price AS DECIMAL(10,2)) <= 0
             THEN TRUE ELSE FALSE END                AS is_price_anomaly,
        CASE WHEN CAST(b.freight_value AS DECIMAL(10,2)) < 0
             THEN TRUE ELSE FALSE END                AS is_freight_anomaly

        {{ silver_metadata() }}

    FROM deduped b
    -- Referential integrity
    INNER JOIN valid_orders o ON b.order_id = o.order_id
    -- Loại giá trị âm không thể hợp lệ
    WHERE CAST(b.price         AS DECIMAL(10,2)) >= 0
      AND CAST(b.freight_value AS DECIMAL(10,2)) >= 0
),

new AS (SELECT * FROM cleaned)

SELECT new.*
FROM new

{% if is_incremental() %}
    {{ anti_join_incremental(this, ['order_id', 'order_item_id']) }}
{% endif %}
