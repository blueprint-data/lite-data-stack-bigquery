{% macro ensure_source_datasets() -%}
    {%- set source_datasets = [target.name ~ '_tap_github'] -%}
    {%- set project = target.project -%}
    {%- set location = target.location if target.location is defined and target.location else 'US' -%}

    {%- for dataset in source_datasets -%}
        {%- set ddl -%}
            CREATE SCHEMA IF NOT EXISTS `{{ project }}.{{ dataset }}`
            OPTIONS(location="{{ location }}")
        {%- endset -%}
        {% if execute %}
            {% do run_query(ddl) %}
        {% endif %}
    {%- endfor -%}
{%- endmacro %}
