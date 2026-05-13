-- models/olist_silver/slv_products.sql

{{
    config(
        unique_key = 'product_id',
        properties = {
            'location': "'" ~ var('silver_location') ~ "/slv_products'",
            'sorted_by': "ARRAY['product_id']"
        },
        merge_update_columns = [
            'product_category_name', 'product_category_name_english',
            'product_weight_g', 'product_length_cm', 'product_height_cm',
            'product_width_cm', 'product_photos_qty', 'product_volume_cm3',
            'is_weight_anomaly', 'is_category_missing', '_silver_processed_at'
        ],
        post_hook = ["{{ tag_snapshot(this) }}"],
        tags = ['olist_silver', 'slv_products']
    )
}}

WITH src AS (
    SELECT * FROM {{ source('olist_bronze', 'products') }}
),

{{ dedup_cte('src', ['product_id']) }},

translations AS (
    SELECT
        product_category_name,
        product_category_name_english
    FROM {{ source('olist_bronze', 'product_category_name_translation') }}
),

cleaned AS (
    SELECT
        p.product_id,
        p.product_category_name,
        COALESCE(t.product_category_name_english, 'unknown') AS product_category_name_english,

        CAST(p.product_weight_g   AS INTEGER) AS product_weight_g,
        CAST(p.product_length_cm  AS INTEGER) AS product_length_cm,
        CAST(p.product_height_cm  AS INTEGER) AS product_height_cm,
        CAST(p.product_width_cm   AS INTEGER) AS product_width_cm,
        CAST(p.product_photos_qty AS INTEGER) AS product_photos_qty,

        -- Volume chỉ tính khi đủ 3 chiều
        CASE
            WHEN p.product_length_cm IS NOT NULL
             AND p.product_height_cm IS NOT NULL
             AND p.product_width_cm  IS NOT NULL
            THEN CAST(p.product_length_cm AS INTEGER)
                 * CAST(p.product_height_cm AS INTEGER)
                 * CAST(p.product_width_cm  AS INTEGER)
            ELSE NULL
        END AS product_volume_cm3,

        CASE WHEN CAST(p.product_weight_g AS INTEGER) <= 0
             THEN TRUE ELSE FALSE END AS is_weight_anomaly,
        CASE WHEN p.product_category_name IS NULL
             THEN TRUE ELSE FALSE END AS is_category_missing

        {{ silver_metadata() }}

    FROM deduped p
    LEFT JOIN translations t ON p.product_category_name = t.product_category_name
),

new AS (SELECT * FROM cleaned)

SELECT new.*
FROM new

{% if is_incremental() %}
    {{ anti_join_incremental(this, ['product_id']) }}
{% endif %}
