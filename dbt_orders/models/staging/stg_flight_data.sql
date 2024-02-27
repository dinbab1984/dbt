{{ config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='parquet',
    location_root='file:///data/staging',
) }}

SELECT 
    ORIGIN_COUNTRY_NAME
    , DEST_COUNTRY_NAME
    , COUNT
/*FROM JSON.`file:///data/flight-data/json` */
FROM {{ source('raw', 'flight_data') }}