-- tests/staging/stg_cores_landings_le_attempts.sql
select
    id as core_id,
    rtls_attempts,
    rtls_landings,
    asds_attempts,
    asds_landings
from {{ ref('stg_cores') }}
where rtls_landings > rtls_attempts or asds_landings > asds_attempts
