SELECT
  EXTRACT(YEAR FROM launched_at) AS launch_year,
  COUNT(*) AS total_launches,
  COUNT(*) FILTER (WHERE 
    CASE
      WHEN launch_success IN ('true', 't', '1') THEN TRUE
      WHEN launch_success IN ('false', 'f', '0') THEN FALSE
      ELSE NULL
    END
  ) AS successful_launches,
  ROUND(
    COUNT(*) FILTER (WHERE 
      CASE
        WHEN launch_success IN ('true', 't', '1') THEN TRUE
        WHEN launch_success IN ('false', 'f', '0') THEN FALSE
        ELSE NULL
      END
    )::numeric / COUNT(*) * 100, 
    2
  ) AS success_rate_pct
FROM {{ ref('stg_launches') }}
GROUP BY launch_year
ORDER BY launch_year
