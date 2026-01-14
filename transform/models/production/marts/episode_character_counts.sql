WITH episodes AS (
    SELECT
        episode_id,
        episode_code,
        character_count,
        created_at
    FROM {{ ref('stg_episodes') }}
)

SELECT *
FROM episodes
