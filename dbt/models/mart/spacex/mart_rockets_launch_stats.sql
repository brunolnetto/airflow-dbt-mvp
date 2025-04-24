-- mart_rockets_launch_stats.sql
WITH rocket_launch_stats AS (
    SELECT
        r.name AS rocket_name,
        COUNT(l.id) AS total_launches,
        SUM(CASE WHEN l.launch_success IN ('true', 't', '1') THEN 1 ELSE 0 END) AS successful_launches,
        CASE 
            WHEN COUNT(l.id) = 0 THEN 0
            ELSE ROUND(SUM(CASE WHEN l.launch_success IN ('true', 't', '1') THEN 1 ELSE 0 END)::numeric / COUNT(l.id) * 100, 2)
        END AS success_rate_pct
    FROM {{ ref('stg_rockets') }} r
    LEFT JOIN {{ ref('stg_launches') }} l ON r.id = l.rocket
    GROUP BY r.name
)
SELECT
    rocket_name,
    total_launches,
    successful_launches,
    success_rate_pct
FROM rocket_launch_stats
ORDER BY rocket_name
