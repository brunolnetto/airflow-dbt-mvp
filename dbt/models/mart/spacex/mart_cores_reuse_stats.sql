-- mart_cores_reuse_stats.sql
WITH cores_data AS (
    SELECT
        serial,
        status,
        reuse_count::numeric,
        rtls_landings::numeric,
        asds_landings::numeric
    FROM {{ ref('stg_cores') }}
)
SELECT
    serial,
    status,
    reuse_count,
    rtls_landings,
    asds_landings
FROM cores_data
WHERE reuse_count > 0
ORDER BY reuse_count DESC
