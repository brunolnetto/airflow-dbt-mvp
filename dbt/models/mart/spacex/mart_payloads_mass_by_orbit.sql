WITH payload_stats AS (
    SELECT
        orbit,
        count(1) AS total_payloads,
        SUM(payload_mass) AS total_mass
    FROM {{ ref('stg_payloads') }}
    GROUP BY orbit
)
SELECT * FROM payload_stats
