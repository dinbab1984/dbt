{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    file_format='json',
    location_root='file:///data/data-mart',
) }}

SELECT 
    ORIGIN_COUNTRY_NAME
    , SUM(COUNT) as TOTAL_FLIGHTS
FROM {{ ref('stg_flight_data') }}
GROUP BY 1