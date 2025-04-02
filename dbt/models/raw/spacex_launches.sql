{{ config(materialized='table') }}

SELECT *
FROM `spacex-data-pipeline.raw.spacex_launches`
