-- dbt/models/staging/stg_spacex_launches.sql

WITH source AS (
  SELECT * FROM {{ source('raw', 'launches') }}
)

SELECT
  id,
  name,
  CAST(date_utc AS TIMESTAMP) AS launched_at,
  CAST(NULLIF(success, 'null') AS BOOL) AS launch_success,  -- Handle "null" strings
  rocket,
  details,
  flight_number
FROM source
