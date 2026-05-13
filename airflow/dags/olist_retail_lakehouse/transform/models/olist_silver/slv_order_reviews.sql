-- models/olist_silver/slv_order_reviews.sql

{{
    config(
        unique_key   = ['review_id', 'order_id'],
        properties = {
            'location': "'" ~ var('silver_location') ~ "/slv_order_reviews'",
            'sorted_by': "ARRAY['review_id', 'order_id']"
        },
        merge_update_columns = [
            'review_score', 'review_comment_title', 'review_comment_message',
            'review_creation_date', 'review_answer_timestamp',
            'is_score_out_of_range', '_silver_processed_at'
        ],
        post_hook = ["{{ tag_snapshot(this) }}"],
        tags = ['olist_silver', 'slv_order_reviews']
    )
}}

WITH src AS (
    SELECT * FROM {{ source('olist_bronze', 'order_reviews') }}
),

{{ dedup_cte('src', ['review_id', 'order_id']) }},

valid_orders AS (
    SELECT DISTINCT order_id FROM {{ ref('slv_orders') }}
),

cleaned AS (
    SELECT
        b.review_id,
        b.order_id,
        CAST(b.review_score AS INTEGER)                 AS review_score,
        TRIM(b.review_comment_title)                    AS review_comment_title,
        TRIM(b.review_comment_message)                  AS review_comment_message,
        CAST(b.review_creation_date   AS TIMESTAMP)     AS review_creation_date,
        CAST(b.review_answer_timestamp AS TIMESTAMP)    AS review_answer_timestamp,

        CASE
            WHEN CAST(b.review_score AS INTEGER) NOT BETWEEN 1 AND 5
            THEN TRUE ELSE FALSE
        END AS is_score_out_of_range

        {{ silver_metadata() }}

    FROM deduped b
    INNER JOIN valid_orders o ON b.order_id = o.order_id
    WHERE b.review_score IS NOT NULL
),

new AS (SELECT * FROM cleaned)

SELECT new.*
FROM new

{% if is_incremental() %}
    {{ anti_join_incremental(this, ['review_id', 'order_id']) }}
{% endif %}
