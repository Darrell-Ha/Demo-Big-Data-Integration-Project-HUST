-- models/olist_silver/slv_order_timeline.sql
-- Flatten timestamps của order thành dạng dọc (1 row / event / order).
-- Giúp tính SLA, lead time, on-time rate dễ hơn ở Gold.

{{
    config(
        unique_key = ['order_id', 'event_type'],
        properties = {
            'location': "'" ~ var('silver_location') ~ "/slv_order_timeline'",
            'sorted_by': "ARRAY['order_id', 'event_type']"
        },
        merge_update_columns = [
            'event_ts', 'event_date', '_silver_processed_at'
        ],
        post_hook    = ["{{ tag_snapshot(this) }}"],
        tags = ['olist_silver', 'slv_order_timeline']
    )
}}

WITH slv_orders AS (
    SELECT
        order_id,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date
    FROM {{ ref('slv_orders') }}
),

unpivoted AS (
    SELECT order_id, 'purchase'           AS event_type, order_purchase_timestamp        AS event_ts FROM slv_orders WHERE order_purchase_timestamp        IS NOT NULL
    UNION ALL
    SELECT order_id, 'approved'           AS event_type, order_approved_at               AS event_ts FROM slv_orders WHERE order_approved_at               IS NOT NULL
    UNION ALL
    SELECT order_id, 'carrier_pickup'     AS event_type, order_delivered_carrier_date    AS event_ts FROM slv_orders WHERE order_delivered_carrier_date    IS NOT NULL
    UNION ALL
    SELECT order_id, 'delivered'          AS event_type, order_delivered_customer_date   AS event_ts FROM slv_orders WHERE order_delivered_customer_date   IS NOT NULL
    UNION ALL
    SELECT order_id, 'estimated_delivery' AS event_type, order_estimated_delivery_date   AS event_ts FROM slv_orders WHERE order_estimated_delivery_date   IS NOT NULL
),

final AS (
    SELECT
        order_id,
        event_type,
        event_ts,
        CAST(event_ts AS DATE) AS event_date,
        current_timestamp      AS _silver_processed_at
    FROM unpivoted
),

new AS (SELECT * FROM final)

SELECT new.*
FROM new

{% if is_incremental() %}
    {{ anti_join_incremental(this, ['order_id', 'event_type']) }}
{% endif %}
