{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    file_format='json',
    location_root='file:///data/dbt/refactoring/legacy',
) }}
select 
 ID
 ,FIRST_NAME
 ,LAST_NAME
from {{ source('raw', 'jaffle_shop_customers') }} 