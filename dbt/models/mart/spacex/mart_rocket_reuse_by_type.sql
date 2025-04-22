WITH rocket_reuse_stats AS (
    SELECT
        type AS rocket_type,
        COUNT(*) AS total_rockets,
        SUM(CASE WHEN reuse_count > 0 THEN 1 ELSE 0 END) AS reused_rockets
    FROM {{ ref('stg_rockets') }}
    GROUP BY rocket_type
)
SELECT
    rocket_type,
    total_rockets,
    reused_rockets,
    ROUND((reused_rockets * 100.0) / total_rockets, 2) AS reuse_rate
FROM rocket_reuse_stats;
