-- dbt/models/staging/stg_spacex_launches.sql

WITH source AS (
  SELECT * FROM {{ source('raw', 'launches') }}
)

SELECT
  id,
  name,
  CAST(date_utc AS TIMESTAMP) AS launched_at,
  CASE
    WHEN LOWER(success::text) = 'true' THEN TRUE
    ELSE FALSE
  END AS launch_success,
  rocket,
  details,
  flight_number
FROM source