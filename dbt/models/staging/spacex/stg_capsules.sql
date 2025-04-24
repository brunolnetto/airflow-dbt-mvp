WITH capsules_raw AS (
    SELECT
        id,
        serial,
        status,
        water_landings,
        land_landings,
        reuse_count,
        launches,
        last_update
    FROM {{ source('raw', 'capsules') }}
)
SELECT * FROM capsules_raw
