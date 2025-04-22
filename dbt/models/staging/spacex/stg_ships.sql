-- stg_ships.sql
WITH source_data AS (
    SELECT
        id,
        name,
        type,
        speed_kn,
        status,
        latitude,
        longitude
    FROM {{ source('raw', 'ships') }}
)
SELECT
    id,
    name,
    type,
    speed_kn,
    status,
    latitude,
    longitude
FROM source_data
