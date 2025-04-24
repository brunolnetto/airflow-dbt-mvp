-- mart_capsules_usage_stats.sql
WITH capsule_usage AS (
    SELECT
        serial::text,
        reuse_count::numeric,
        water_landings::numeric,
        land_landings::numeric
    FROM {{ ref('stg_capsules') }}
)
SELECT
    serial,
    reuse_count,
    water_landings,
    land_landings
FROM capsule_usage
WHERE reuse_count > 0
ORDER BY reuse_count DESC
