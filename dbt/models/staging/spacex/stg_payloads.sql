WITH payloads_raw AS (
    SELECT
        payload_id,
        payload_name,
        CAST(mass_kg AS INT) AS payload_mass,
        payload_type,
        orbit,
        manufacturer
    FROM {{ source('raw', 'payloads') }}
)
SELECT * FROM payloads_raw
