WITH characters AS (
    SELECT *
    FROM {{ ref('stg_characters') }}
)

SELECT
    coalesce(status, 'unknown') AS status,
    count(*) AS character_count
FROM characters
GROUP BY status
