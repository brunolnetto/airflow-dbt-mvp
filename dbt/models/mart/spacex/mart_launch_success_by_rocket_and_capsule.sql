-- mart_launch_success_by_rocket_and_capsule.sql
WITH launch_stats AS (
    SELECT
        l.id AS launch_id,
        l.launched_at,
        l.launch_success,
        r.name AS rocket_name,
        c.serial AS capsule_serial
    FROM {{ ref('stg_launches') }} l
    LEFT JOIN {{ ref('stg_rockets') }} r ON l.rocket = r.id
    LEFT JOIN {{ ref('stg_capsules') }} c ON l.id = c.launches
)
SELECT
    rocket_name,
    capsule_serial,
    COUNT(*) AS total_launches,
    COUNT(*) FILTER (WHERE launch_success IN ('true', 't', '1')) AS successful_launches,
    ROUND(
        COUNT(*) FILTER (WHERE launch_success IN ('true', 't', '1'))::numeric / COUNT(*) * 100, 
        2
    ) AS success_rate_pct
FROM launch_stats
GROUP BY rocket_name, capsule_serial
ORDER BY rocket_name, capsule_serial;
