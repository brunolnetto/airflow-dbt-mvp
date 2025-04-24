WITH launch_stats AS (
    SELECT
        EXTRACT(YEAR FROM launched_at) AS launch_year,
        COUNT(*) AS total_launches,
        SUM(CASE WHEN was_successful THEN 1 ELSE 0 END) AS successful_launches
    FROM {{ ref('stg_launches') }}
    GROUP BY launch_year
)
SELECT
    launch_year,
    total_launches,
    successful_launches,
    case 
        when total_launches <> 0 
            then 100.0*ROUND(1.0 * successful_launches / total_launches, 2) 
        else 0
    end AS success_rate
FROM launch_stats
