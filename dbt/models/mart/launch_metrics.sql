SELECT
  EXTRACT(YEAR FROM launched_at) AS launch_year,
  COUNT(*) AS total_launches,
  COUNT(*) FILTER (WHERE launch_success) AS successful_launches,
  ROUND(
    COUNT(*) FILTER (WHERE launch_success)::numeric / COUNT(*) * 100, 
    2
  ) AS success_rate_pct
FROM {{ ref('stg_spacex_launches') }}
GROUP BY launch_year
ORDER BY launch_year
