-- CI smoke test: no data change
WITH source AS (
    SELECT *
    FROM {{ source('rick_and_morty', 'episodes') }}
)

SELECT
    id::int AS episode_id,
    name,
    air_date,
    episode AS episode_code,
    url AS episode_url,
    created::timestamptz AS created_at,
    COALESCE(JSONB_ARRAY_LENGTH(characters::jsonb), 0) AS character_count
FROM source
