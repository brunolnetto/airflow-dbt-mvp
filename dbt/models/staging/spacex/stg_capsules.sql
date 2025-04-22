WITH capsules_raw AS (
    SELECT
        capsule_id,
        capsule_serial,
        status,
        reuse_count,
        last_update
    FROM {{ source('raw', 'capsules') }}
)
SELECT * FROM capsules_raw
