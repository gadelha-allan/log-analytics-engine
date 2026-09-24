{% macro generate_schema_name(custom_schema_name, node) -%}
    {# Keep Medallion layer schemas stable instead of prefixing them with `main_`. #}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
