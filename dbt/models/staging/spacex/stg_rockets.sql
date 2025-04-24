WITH rockets_raw AS (
    SELECT
        id,
        name,
        type,
        first_stage,
        second_stage
    FROM {{ source('raw', 'rockets') }}
)
SELECT * FROM rockets_raw
