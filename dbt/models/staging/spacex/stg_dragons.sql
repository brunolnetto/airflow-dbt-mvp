-- stg_dragons.sql
WITH source_data AS (
    SELECT
        id,
        name,
        type,
        active,
        crew_capacity,
        dry_mass_kg,
        height_w_trunk,
        diameter,
        first_flight,
        flickr_images
    FROM {{ source('raw', 'dragons') }}
)
SELECT
    id,
    name,
    type,
    active,
    crew_capacity,
    dry_mass_kg,
    height_w_trunk,
    diameter,
    first_flight,
    flickr_images
FROM source_data
