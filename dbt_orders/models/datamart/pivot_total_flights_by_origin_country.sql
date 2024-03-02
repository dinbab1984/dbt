{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    file_format='json',
    location_root='file:///data/data-mart',
) }}

{% set get_countries_query %}
  select distinct ORIGIN_COUNTRY_NAME FROM {{ source('raw', 'flight_data') }}
{% endset %}
{% set results = run_query(get_countries_query) %}
{% if execute %}
 {# Return the first column #}
 {% set countries = results.columns[0].values() %}
 {% else %}
 {% set countries =[]%}
{% endif %}

with orig_country as
(
SELECT 
    ORIGIN_COUNTRY_NAME
    , SUM(COUNT) as COUNT
FROM {{ source('raw', 'flight_data') }}

)
SELECT 
    {% for country in countries %}
    sum(case when ORIGIN_COUNTRY_NAME = '{{country}}' then COUNT end) as "{{country}}_COUNT",
    {% endfor %}
    sum(COUNT) as total_count
FROM orig_country