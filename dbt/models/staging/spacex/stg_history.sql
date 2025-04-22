-- stg_history.sql
WITH source_data AS (
    SELECT
        id,
        title,
        event_date_utc,
        event_date_unix,
        details
    FROM {{ source('raw', 'history') }}
)
SELECT
    id,
    title,
    event_date_utc,
    event_date_unix,
    details
FROM source_data
