{% test positive_value_or_zero(model, column_name) %}

SELECT
    *
FROM {{ model }}
WHERE {{ column_name }} < 0
   OR {{ column_name }} IS NULL

{% endtest %}
