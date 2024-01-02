{# A basic example for a project-wide macro to cast a column uniformly #}

{% macro cents_to_dollars(column_name, precision=2) -%}
    ({{ column_name }} / 100)::{{ type_numeric() }}(16, {{ precision }})
{%- endmacro %}


{% macro dollars_to_cents(column_name) -%}
    ({{ column_name }} * 100)::{{ type_numeric() }}(16, 0)
{%- endmacro %}
