-- mart_ships_location_stats.sql
WITH ships_location AS (
    SELECT
        name,
        latitude,
        longitude,
        status
    FROM {{ ref('stg_ships') }}
)
SELECT
    name,
    latitude,
    longitude,
    status
FROM ships_location
WHERE status = 'active'
ORDER BY latitude, longitude;
