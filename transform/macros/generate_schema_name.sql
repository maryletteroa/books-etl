{% macro generate_schema_name(custom_schema_name, node) %}
    {# If a schema is defined, use it exactly as written #}
    {% if custom_schema_name is not none %}
        {{ custom_schema_name | trim }}
    {% else %}
        {{ target.schema }}
    {% endif %}
{% endmacro %}
