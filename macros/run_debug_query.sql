{# 例: dbt run-operation run_debug_query --args '{"query": "SELECT ..."}' #}
{% macro run_debug_query(query) %}
  {% set results = run_query(query) %}
  {% if results|length > 0 %}
    {% do results.print_json(indent=2) %}
  {% else %}
    {% do log('No rows available.', info=True) %}
  {% endif %}
{% endmacro %}
