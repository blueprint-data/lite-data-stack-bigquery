WITH source AS (
    SELECT *
    FROM {{ source('rick_and_morty', 'locations') }}
)

SELECT
    id::int AS location_id,
    name,
    dimension,
    url AS location_url,
    created::timestamptz AS created_at,
    nullif(type, '') AS location_type,
    coalesce(jsonb_array_length(residents::jsonb), 0) AS resident_count
FROM source
