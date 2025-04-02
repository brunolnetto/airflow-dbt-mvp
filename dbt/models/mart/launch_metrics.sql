-- dbt/models/mart/launch_metrics.sql

SELECT
  EXTRACT(YEAR FROM launched_at) AS launch_year,
  COUNT(*) AS total_launches,
  COUNTIF(launch_success) AS successful_launches,
  ROUND(COUNTIF(launch_success) / COUNT(*) * 100, 2) AS success_rate_pct
FROM {{ ref('stg_spacex_launches') }}
GROUP BY launch_year
ORDER BY launch_year
