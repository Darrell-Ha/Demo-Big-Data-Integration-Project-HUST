{% macro date_filter(column_name) %}
  {% set run_date = var('run_date', none) %}
  {% set start_date = var('start_date', none) %}
  {% set end_date = var('end_date', none) %}

  {% if start_date and end_date %}
    {{ column_name }} between
      date('{{ start_date }}')
      and date('{{ end_date }}')

  {% elif run_date %}
    {{ column_name }} = date('{{ run_date }}')

  {% else %}
    1 = 1
  {% endif %}
{% endmacro %}

{% macro validate_date_vars() %}
  {% set run_date = var('run_date', none) %}
  {% set start_date = var('start_date', none) %}
  {% set end_date = var('end_date', none) %}

  {% if start_date and not end_date %}
    {{ exceptions.raise_compiler_error(
      "start_date is provided but end_date is missing"
    ) }}
  {% endif %}

  {% if end_date and not start_date %}
    {{ exceptions.raise_compiler_error(
      "end_date is provided but start_date is missing"
    ) }}
  {% endif %}

  {% if run_date and (start_date or end_date) %}
    {{ exceptions.raise_compiler_error(
      "Use either run_date OR start_date/end_date, not both"
    ) }}
  {% endif %}
{% endmacro %}