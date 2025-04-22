-- stg_landpads.sql
WITH source_data AS (
    SELECT
        id,
        name,
        status,
        locality,
        latitude,
        longitude
    FROM {{ source('raw', 'landpads') }}
)
SELECT
    id,
    name,
    status,
    locality,
    latitude,
    longitude
FROM source_data
