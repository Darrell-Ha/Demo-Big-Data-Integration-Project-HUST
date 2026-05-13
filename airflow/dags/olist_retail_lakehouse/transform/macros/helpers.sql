-- macros/helpers.sql

{#
  silver_metadata()
  Thêm các cột tracking lineage từ Bronze sang Silver.
  Gọi ở cuối SELECT list, có dấu phẩy đầu dòng.
  Ví dụ:
      SELECT
          col1,
          col2
          {{ silver_metadata() }}
      FROM ...
#}
{% macro silver_metadata() %}
    , _ingested_at
    , _batch_id
    , _pipeline_version
    , current_timestamp AS _silver_processed_at
{% endmacro %}


{#
  anti_join_incremental(this, keys)
  Thay thế tuple NOT IN (không hỗ trợ trên Trino) bằng LEFT ANTI JOIN.
  keys: list tên column tạo thành composite key.

  Ví dụ:
      {{ anti_join_incremental(this, ['order_id', 'order_item_id']) }}
  Sinh ra:
      LEFT JOIN iceberg.silver.order_items AS _existing
          ON  new.order_id       = _existing.order_id
          AND new.order_item_id  = _existing.order_item_id
      WHERE _existing.order_id IS NULL
#}
{% macro anti_join_incremental(target_relation, keys) %}
    LEFT JOIN {{ target_relation }} AS _existing
        ON  {% for key in keys %}
                new.{{ key }} = _existing.{{ key }}
                {% if not loop.last %} AND {% endif %}
            {% endfor %}
    WHERE _existing.{{ keys[0] }} IS NULL
{% endmacro %}


{#
  dedup_cte(source_ref, partition_keys)
  ROW_NUMBER dedup trên business key, giữ row _ingested_at mới nhất.
  Trả về tên CTE 'deduped' để caller dùng tiếp.

  Ví dụ:
      WITH src AS (SELECT * FROM {{ source('bronze','orders') }}),
      {{ dedup_cte('src', ['order_id']) }}
      SELECT ... FROM deduped
#}
{% macro dedup_cte(src_alias, partition_keys) %}
deduped AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY {{ partition_keys | join(', ') }}
                ORDER BY _ingested_at DESC
            ) AS _rn
        FROM {{ src_alias }}
    )
    WHERE _rn = 1
)
{% endmacro %}


{#
  tag_snapshot(model_name)
  post_hook chuẩn để gắn metadata vào Iceberg snapshot.
  Dùng trong config() của từng model:
      post_hook = ["{{ tag_snapshot(this) }}"]
#}
{% macro tag_snapshot(relation) %}
    ALTER TABLE {{ relation }}
    SET PROPERTIES extra_properties = MAP(
        ARRAY['snapshot.summary.dbt_run_id', 'snapshot.summary.dbt_model', 'snapshot.summary.pipeline_version'],
        ARRAY['{{ invocation_id }}','{{ relation.identifier }}','{{ var("pipeline_version") }}']
    )
{% endmacro %}


{#
  brazil_states()
  Trả về Jinja list 27 bang Brazil — dùng chung cho customers và sellers.
#}
{% macro brazil_states() %}
    {% set states = [
        'AC','AL','AP','AM','BA','CE','DF','ES','GO',
        'MA','MT','MS','MG','PA','PB','PR','PE','PI',
        'RJ','RN','RS','RO','RR','SC','SP','SE','TO'
    ] %}
    {{ return(states) }}
{% endmacro %}
