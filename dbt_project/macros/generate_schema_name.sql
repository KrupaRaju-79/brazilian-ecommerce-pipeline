-- This macro overrides dbt's default schema naming behaviour.
-- Without this, dbt concatenates the profile dataset + model schema
-- producing names like "staging_mart" instead of just "mart".
-- With this macro, models write directly to the schema defined in dbt_project.yml.

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}