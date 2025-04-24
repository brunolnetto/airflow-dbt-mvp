WITH rocket_reuse_stats AS (
    SELECT
        type AS rocket_type,
        SUM(
            CASE
                WHEN ((first_stage::jsonb ->> 'reusable')::boolean OR 
                (second_stage::jsonb ->> 'reusable')::boolean) THEN 1
                ELSE 0
            END
        ) AS reused_rockets,
        COUNT(*) AS total_rockets
    FROM {{ ref('stg_rockets') }}
    GROUP BY rocket_type
)
SELECT
    rocket_type,
    total_rockets,
    case
        when total_rockets <> 0
        then ROUND((reused_rockets * 100.0) / total_rockets, 2)
        else 0
    end AS reuse_rate
FROM rocket_reuse_stats
