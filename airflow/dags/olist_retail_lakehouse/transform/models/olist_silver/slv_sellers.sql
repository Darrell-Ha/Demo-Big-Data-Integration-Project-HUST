-- models/olist_silver/slv_sellers.sql

{{
    config(
        unique_key = 'seller_id',
        properties = {
            'location': "'" ~ var('silver_location') ~ "/slv_sellers'",
            'sorted_by': "ARRAY['seller_id']"
        },
        merge_update_columns = [
            'seller_zip_code_prefix', 'seller_city',
            'seller_state', 'is_invalid_state', '_silver_processed_at'
        ],
        post_hook = ["{{ tag_snapshot(this) }}"],
        tags = ['olist_silver', 'slv_sellers']
    )
}}

WITH src AS (
    SELECT * FROM {{ source('olist_bronze', 'sellers') }}
),

{{ dedup_cte('src', ['seller_id']) }},

cleaned AS (
    SELECT
        seller_id,
        LPAD(CAST(seller_zip_code_prefix AS VARCHAR), 5, '0') AS seller_zip_code_prefix,
        LOWER(TRIM(seller_city))                              AS seller_city,
        UPPER(TRIM(seller_state))                             AS seller_state,

        CASE
            WHEN UPPER(TRIM(seller_state)) NOT IN (
                {{ "'" + brazil_states() | join("','") + "'" }}
            )
            THEN TRUE ELSE FALSE
        END AS is_invalid_state

        {{ silver_metadata() }}

    FROM deduped
    WHERE seller_id IS NOT NULL
),

new AS (SELECT * FROM cleaned)

SELECT new.*
FROM new

{% if is_incremental() %}
    {{ anti_join_incremental(this, ['seller_id']) }}
{% endif %}
