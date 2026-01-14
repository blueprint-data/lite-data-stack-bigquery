WITH locations AS (
    SELECT *
    FROM {{ ref('stg_locations') }}
)

SELECT
    location_id,
    name,
    location_type,
    dimension,
    resident_count
FROM locations
