WITH payloads_raw AS (
    SELECT
        id,
        name,
        CASE
            WHEN mass_kg ~ '^\d+$' THEN CAST(mass_kg AS INT)
            ELSE NULL
        END AS payload_mass,
        type,
        orbit,
        manufacturers
    FROM {{ source('raw', 'payloads') }}
)
SELECT * FROM payloads_raw
