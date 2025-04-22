WITH launch_stats AS (
    SELECT
        EXTRACT(YEAR FROM launch_date) AS launch_year,
        COUNT(*) AS total_launches,
        SUM(CASE WHEN was_successful THEN 1 ELSE 0 END) AS successful_launches
    FROM {{ ref('stg_launches') }}
    GROUP BY launch_year
)
SELECT
    launch_year,
    total_launches,
    successful_launches,
    ROUND((successful_launches * 100.0) / total_launches, 2) AS success_rate
FROM launch_stats;
