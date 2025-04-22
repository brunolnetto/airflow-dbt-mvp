WITH capsule_status_stats AS (
    SELECT
        status,
        COUNT(*) AS total_capsules
    FROM {{ ref('stg_capsules') }}
    GROUP BY status
)
SELECT * FROM capsule_status_stats;
