WITH payload_stats AS (
    SELECT
        orbit,
        SUM(payload_mass) AS total_mass
    FROM {{ ref('stg_payloads') }}
    GROUP BY orbit
)
SELECT * FROM payload_stats;
