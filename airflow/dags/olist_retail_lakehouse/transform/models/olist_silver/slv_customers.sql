-- models/olist_silver/slv_customers.sql

{{
    config(
        unique_key = 'customer_id',
        properties = {
            'location': "'" ~ var('silver_location') ~ "/slv_customers'",
            'sorted_by': "ARRAY['customer_id']"
        },
        merge_update_columns = [
            'customer_unique_id', 'customer_zip_code_prefix',
            'customer_city', 'customer_state', 'is_invalid_state','_silver_processed_at'
        ],
        post_hook = ["{{ tag_snapshot(this) }}"],
        tags = ['olist_silver', 'slv_customers']
    )
}}

WITH src AS (
    SELECT *
    FROM {{ source('olist_bronze', 'customers') }}
),

{{ dedup_cte('src', ['customer_id']) }},

cleaned AS (
    SELECT
        customer_id,
        customer_unique_id,
        LPAD(CAST(customer_zip_code_prefix AS VARCHAR), 5, '0') AS customer_zip_code_prefix,
        LOWER(TRIM(customer_city))                              AS customer_city,
        UPPER(TRIM(customer_state))                             AS customer_state,

        -- DQ flag: state không thuộc 27 bang Brazil
        CASE
            WHEN UPPER(TRIM(customer_state)) NOT IN (
                {{ "'" + brazil_states() | join("','") + "'" }}
            )
            THEN TRUE ELSE FALSE
        END AS is_invalid_state

        {{ silver_metadata() }}

    FROM deduped
    WHERE customer_state IS NOT NULL
),

new AS (SELECT * FROM cleaned)

SELECT new.*
FROM new

{% if is_incremental() %}
    {{ anti_join_incremental(this, ['customer_id']) }}
{% endif %}
