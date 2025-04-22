WITH rockets_raw AS (
    SELECT
        rocket_id,
        rocket_name,
        type,
        first_stage_engines,
        second_stage_engines
    FROM {{ source('raw', 'rockets') }}
)
SELECT * FROM rockets_raw
