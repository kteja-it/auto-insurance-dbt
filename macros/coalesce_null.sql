{% macro coalesce_null(column_name,default_value)   %}
    COALESCE(NULLIF( {{column_name}},'' ), {{default_value }} ) AS {{column_name}}
{% endmacro %}