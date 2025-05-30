-- tests/mart/mart_launches_success_by_year_successful_le_total.sql
select
    launch_year,
    total_launches,
    successful_launches
from {{ ref('mart_launches_success_by_year') }}
where successful_launches > total_launches
