{% macro convert_card_number_to_string(card_number) %}
    CASE
        WHEN FLOOR({{ card_number }}::NUMBER / 4) = 12 THEN 'A'
        WHEN FLOOR({{ card_number }}::NUMBER / 4) = 11 THEN 'K'
        WHEN FLOOR({{ card_number }}::NUMBER / 4) = 10 THEN 'Q'
        WHEN FLOOR({{ card_number }}::NUMBER / 4) = 9 THEN 'J'
        WHEN FLOOR({{ card_number }}::NUMBER / 4) = 8 THEN 'T'
        ELSE TO_CHAR(FLOOR({{ card_number }}::NUMBER / 4) + 2)
    END ||
    CASE
        WHEN {{ card_number }}::NUMBER % 4 = 0 THEN 's'
        WHEN {{ card_number }}::NUMBER % 4 = 1 THEN 'h'
        WHEN {{ card_number }}::NUMBER % 4 = 2 THEN 'd'
        WHEN {{ card_number }}::NUMBER % 4 = 3 THEN 'c'
    END
{% endmacro %}

{% macro convert_card_array_to_strings(key_of_card_number) %}
    array_agg(
        {{ convert_card_number_to_string(key_of_card_number) }}
    )
{% endmacro %}
