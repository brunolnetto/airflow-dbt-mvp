-- mart_history_by_launch_year.sql
WITH launch_history AS (
    SELECT
        EXTRACT(YEAR FROM l.launched_at) AS launch_year,
        h.title AS history_title,
        h.event_date_utc AS event_date_utc
    FROM {{ ref('stg_launches') }} l
    LEFT JOIN {{ ref('stg_history') }} h ON l.id = h.id
)
SELECT
    launch_year,
    history_title,
    event_date_utc
FROM launch_history
ORDER BY launch_year, event_date_utc;
