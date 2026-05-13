-- models/olist_silver/slv_order_payments.sql

{{
    config(
        unique_key  = ['order_id', 'payment_sequential'],
        properties = {
            'location': "'" ~ var('silver_location') ~ "/slv_order_payments'",
            'sorted_by': "ARRAY['order_id', 'payment_sequential']"
        },
        merge_update_columns = [
            'payment_type', 'payment_installments',
            'payment_value', 'is_payment_anomaly', '_silver_processed_at'
        ],
        post_hook = ["{{ tag_snapshot(this) }}"],
        tags = ['olist_silver', 'slv_order_payments']
    )
}}

WITH src AS (
    SELECT * FROM {{ source('olist_bronze', 'order_payments') }}
),

{{ dedup_cte('src', ['order_id', 'payment_sequential']) }},

valid_orders AS (
    SELECT DISTINCT order_id FROM {{ ref('slv_orders') }}
),

cleaned AS (
    SELECT
        b.order_id,
        b.payment_sequential,
        LOWER(TRIM(b.payment_type))              AS payment_type,
        CAST(b.payment_installments AS INTEGER)  AS payment_installments,
        CAST(b.payment_value AS DECIMAL(10,2))   AS payment_value,

        CASE WHEN CAST(b.payment_value AS DECIMAL(10,2)) < 0
             THEN TRUE ELSE FALSE END            AS is_payment_anomaly

        {{ silver_metadata() }}

    FROM deduped b
    INNER JOIN valid_orders o ON b.order_id = o.order_id
    WHERE CAST(b.payment_value AS DECIMAL(10,2)) >= 0
),

new AS (SELECT * FROM cleaned)

SELECT new.*
FROM new

{% if is_incremental() %}
    {{ anti_join_incremental(this, ['order_id', 'payment_sequential']) }}
{% endif %}
