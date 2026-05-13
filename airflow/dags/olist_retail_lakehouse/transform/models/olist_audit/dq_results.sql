-- models/audit/dq_results.sql
-- Bảng lưu kết quả DQ check qua các layer và pipeline run.
-- Rows được INSERT vào qua dbt tests kết hợp với store_failures.

{{
    config(
        materialized = 'table',
        properties = {
            'location': "'" ~ var('audit_location') ~ "/dq_results'",
            'sorted_by': "ARRAY['run_id']"
        }
    )
}}

SELECT
    CAST(NULL AS VARCHAR)   AS run_id,
    CAST(NULL AS VARCHAR)   AS layer,
    CAST(NULL AS VARCHAR)   AS table_name,
    CAST(NULL AS VARCHAR)   AS rule_name,
    CAST(NULL AS VARCHAR)   AS status,
    CAST(NULL AS BIGINT)    AS total_count,
    CAST(NULL AS BIGINT)    AS failed_count,
    CAST(NULL AS DOUBLE)    AS failed_pct,
    CAST(NULL AS DOUBLE)    AS threshold,
    CAST(NULL AS TIMESTAMP) AS checked_at
WHERE 1 = 0
