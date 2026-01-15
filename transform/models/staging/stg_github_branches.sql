{{ config(
    materialized='incremental',
    unique_key='branch_sk',
    tags=['github']
) }}

WITH source_data AS (
    SELECT s.*
    FROM {{ source('tap_github', 'branches') }} AS s
    {% if is_incremental() %}
        WHERE s._sdc_extracted_at > (
            SELECT MAX(t._sdc_extracted_at)
            FROM {{ this }} AS t
        )
    {% endif %}
),

parsed AS (
    SELECT
        -- Stitch metadata
        s._sdc_extracted_at,
        s._sdc_received_at,
        s._sdc_batched_at,
        s._sdc_deleted_at,
        s._sdc_sequence,
        s._sdc_table_version,

        -- Raw JSON
        s.data AS data_json
    FROM source_data AS s
),

exploded AS (
    SELECT
        -- Repo identifiers
        p._sdc_extracted_at,
        p._sdc_received_at,
        p._sdc_batched_at,

        -- Branch fields
        p._sdc_deleted_at,
        p._sdc_sequence,

        -- Commit object inside branch
        p._sdc_table_version,
        JSON_VALUE(p.data_json, '$.org') AS org,

        -- Stitch metadata
        JSON_VALUE(p.data_json, '$.repo') AS repo,
        SAFE_CAST(JSON_VALUE(p.data_json, '$.repo_id') AS INT64) AS repo_id,
        JSON_VALUE(p.data_json, '$.name') AS branch_name,
        SAFE_CAST(JSON_VALUE(p.data_json, '$.protected') AS BOOL) AS is_protected,
        JSON_VALUE(p.data_json, '$.commit.sha') AS commit_sha,
        JSON_VALUE(p.data_json, '$.commit.url') AS commit_api_url
    FROM parsed AS p
),

deduped AS (
    SELECT *
    FROM (
        SELECT
            e.*,
            ROW_NUMBER() OVER (
                PARTITION BY
                    e.org,
                    e.repo,
                    e.repo_id,
                    e.branch_name
                ORDER BY
                    e._sdc_extracted_at DESC
            ) AS row_num
        FROM exploded AS e
    )
    WHERE row_num = 1
),

final AS (
    SELECT
        -- Surrogate key (stable)
        org,

        -- Natural keys
        repo,
        repo_id,
        branch_name,
        is_protected,

        -- Attributes
        commit_sha,
        commit_api_url,
        _sdc_extracted_at,

        -- Convenience
        _sdc_received_at,

        -- Stitch metadata
        _sdc_batched_at,
        _sdc_deleted_at,
        _sdc_sequence,
        _sdc_table_version,
        CONCAT(
            COALESCE(org, ''),
            '/',
            COALESCE(repo, ''),
            ':',
            COALESCE(CAST(repo_id AS STRING), ''),
            ':',
            COALESCE(branch_name, '')
        ) AS branch_sk,
        CONCAT(org, '/', repo) AS full_repo_name,

        CURRENT_TIMESTAMP() AS dbt_updated_at
    FROM deduped
)

SELECT *
FROM final
WHERE
    branch_name IS NOT NULL
    AND org IS NOT NULL
    AND repo IS NOT NULL
