-- mart_capsules_usage_stats.sql
WITH capsule_usage AS (
    SELECT
        serial,
        reuse_count,
        water_landings,
        land_landings
    FROM {{ ref('stg_capsules') }}
)
SELECT
    serial,
    reuse_count,
    water_landings,
    land_landings
FROM capsule_usage
WHERE reuse_count > 0
ORDER BY reuse_count DESC;
