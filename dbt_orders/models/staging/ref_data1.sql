{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    file_format='json',
    location_root='file:///data/staging',
) }}

SELECT 
   count(*) as count
/*FROM JSON.`file:///data/flight-data/json` */
FROM {{ source('reference', 'ref_data1') }}