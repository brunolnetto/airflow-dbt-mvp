-- stg_cores.sql
WITH source_data AS (
    SELECT
        id,
        serial,
        status,
        reuse_count,
        rtls_attempts,
        rtls_landings,
        asds_attempts,
        asds_landings,
        last_update,
        launches
    FROM {{ source('raw', 'cores') }}
)
SELECT
    id,
    serial,
    status,
    reuse_count,
    rtls_attempts,
    rtls_landings,
    asds_attempts,
    asds_landings,
    last_update,
    launches
FROM source_data
