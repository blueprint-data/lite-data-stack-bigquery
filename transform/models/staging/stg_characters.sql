WITH source AS (
    SELECT *
    FROM {{ source('rick_and_morty', 'characters') }}
),

renamed AS (
    SELECT
        id::int AS character_id,
        name,
        status,
        species,
        gender,
        origin_name,
        origin_url,
        location_name,
        location_url,
        image AS image_url,
        url AS character_url,
        created::timestamptz AS created_at,
        nullif(type, '') AS character_type
    FROM source
)

SELECT *
FROM renamed
