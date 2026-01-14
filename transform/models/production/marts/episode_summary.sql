WITH episodes AS (
    SELECT *
    FROM {{ ref('stg_episodes') }}
)

SELECT
    episode_id,
    episode_code,
    name,
    air_date,
    character_count
FROM episodes
