-- models/olist_silver/slv_orders.sql

{{
    config(
        unique_key   = 'order_id',
        properties = {
            'location': "'" ~ var('silver_location') ~ "/slv_orders'",
            'sorted_by': "ARRAY['order_id']",
            'partitioning': "ARRAY['order_purchase_date']"
        },
        merge_update_columns = [
            'customer_id', 'order_status',
            'order_purchase_timestamp', 'order_approved_at',
            'order_delivered_carrier_date', 'order_delivered_customer_date',
            'order_estimated_delivery_date', 'order_purchase_date',
            'is_missing_approval', 'is_delivery_before_purchase',
            '_silver_processed_at'
        ],
        post_hook = ["{{ tag_snapshot(this) }}"],
        tags = ['olist_silver', 'slv_orders']
    )
}}

{{ validate_date_vars() }}

WITH src AS (
    SELECT * FROM {{ source('olist_bronze', 'orders') }}
    {% if is_incremental() %}
    WHERE {{ date_filter('order_purchase_timestamp_date') }}
    {% endif %}
),

{{ dedup_cte('src', ['order_id']) }},

cleaned AS (
    SELECT
        order_id,
        customer_id,
        order_status,

        -- Trino: TIMESTAMP columns — Bronze đã lưu dạng TIMESTAMP nên cast trực tiếp
        CAST(order_purchase_timestamp        AS TIMESTAMP) AS order_purchase_timestamp,
        CAST(order_approved_at               AS TIMESTAMP) AS order_approved_at,
        CAST(order_delivered_carrier_date    AS TIMESTAMP) AS order_delivered_carrier_date,
        CAST(order_delivered_customer_date   AS TIMESTAMP) AS order_delivered_customer_date,
        CAST(order_estimated_delivery_date   AS TIMESTAMP) AS order_estimated_delivery_date,

        -- Partition column
        CAST(order_purchase_timestamp AS DATE)             AS order_purchase_date,

        -- DQ flags: không xóa row, chỉ đánh dấu để downstream biết
        CASE
            WHEN order_approved_at IS NULL
             AND order_status NOT IN ('created', 'canceled')
            THEN TRUE ELSE FALSE
        END AS is_missing_approval,

        CASE
            WHEN order_delivered_customer_date IS NOT NULL
             AND order_delivered_customer_date < order_purchase_timestamp
            THEN TRUE ELSE FALSE
        END AS is_delivery_before_purchase

        {{ silver_metadata() }}

    FROM deduped
    -- Loại duy nhất row có logical impossibility vật lý rõ ràng
    WHERE NOT (
        order_delivered_customer_date IS NOT NULL
        AND order_delivered_customer_date < order_purchase_timestamp
    )
),

new AS (SELECT * FROM cleaned)

SELECT new.*
FROM new

{% if is_incremental() %}
    {{ anti_join_incremental(this, ['order_id']) }}
{% endif %}
