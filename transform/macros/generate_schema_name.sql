{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- set target_name = target.name -%}

    {# Extract user from DBT_USER env var #}
    {%- set raw_user = env_var("DBT_USER", "default_user") -%}
    {%- set user = raw_user.split('@')[0] | replace('.', '_') | replace('-', '_') -%}

    {# Production/CI: use configured schema or custom_schema_name #}
    {%- if target_name in ['prod', 'ci'] -%}
        {%- if custom_schema_name is not none -%}
            {{ custom_schema_name | trim }}
        {%- else -%}
            {{ default_schema }}
        {%- endif -%}

    {# Dev: use sandbox schema with defer support #}
    {%- elif target_name == 'dev' -%}

        {# When --defer flag is used, use production dataset names #}
        {%- if flags.DEFER -%}
            {%- if custom_schema_name is not none -%}
                {{ custom_schema_name | trim }}
            {%- else -%}
                {{ default_schema }}
            {%- endif -%}

        {# Normal dev: use sandbox schema #}
        {%- else -%}
            {# Local development: use user-specific sandbox schema #}
            SANDBOX_{{ user | upper }}
        {%- endif -%}

    {%- else -%}
        {# Fallback for any other target #}
        {%- if custom_schema_name is not none -%}
            {{ custom_schema_name | trim }}
        {%- else -%}
            {{ default_schema }}
        {%- endif -%}
    {%- endif -%}

{%- endmacro %}
