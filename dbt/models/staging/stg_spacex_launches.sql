-- dbt/models/staging/stg_spacex_launches.sql

WITH source AS (
  SELECT * FROM {{ source('raw', 'spacex_launches') }}
)

SELECT
  id,
  name,
  CAST(date_utc AS TIMESTAMP) AS launched_at,
  CAST(success AS BOOL) AS launch_success,  -- ensures this column exists
  rocket,
  details,
  flight_number
FROM source
