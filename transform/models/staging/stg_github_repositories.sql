{{ config(
    materialized='incremental',
    unique_key='repo_sk',
    tags=['github']
) }}

WITH source_data AS (
    SELECT s.*
    FROM {{ source('tap_github', 'repositories') }} AS s
    {% if is_incremental() %}
        WHERE s._sdc_extracted_at > (
            SELECT MAX(t._sdc_extracted_at)
            FROM {{ this }} AS t
        )
    {% endif %}
),

parsed AS (
    SELECT
        s._sdc_extracted_at,
        s._sdc_received_at,
        s._sdc_batched_at,
        s._sdc_deleted_at,
        s._sdc_sequence,
        s._sdc_table_version,

        s.data AS data_json
    FROM source_data AS s
),

flattened AS (
    SELECT
        -- Identifiers
        p._sdc_extracted_at,
        p._sdc_received_at,
        p._sdc_batched_at,
        p._sdc_deleted_at,

        -- "org" and "repo" also appear in raw, keep as canonical partition keys
        p._sdc_sequence,
        p._sdc_table_version,

        -- URLs
        SAFE_CAST(JSON_VALUE(p.data_json, '$.id') AS INT64) AS repo_id,
        JSON_VALUE(p.data_json, '$.node_id') AS node_id,
        JSON_VALUE(p.data_json, '$.name') AS repo,
        JSON_VALUE(p.data_json, '$.full_name') AS full_name,
        JSON_VALUE(p.data_json, '$.org') AS org,

        -- Flags
        JSON_VALUE(p.data_json, '$.repo') AS repo_slug,
        JSON_VALUE(p.data_json, '$.html_url') AS repo_github_url,
        JSON_VALUE(p.data_json, '$.git_url') AS git_url,
        JSON_VALUE(p.data_json, '$.ssh_url') AS ssh_url,

        -- Basic metadata
        JSON_VALUE(p.data_json, '$.clone_url') AS clone_url,
        JSON_VALUE(p.data_json, '$.homepage') AS homepage_url,
        SAFE_CAST(JSON_VALUE(p.data_json, '$.private') AS BOOL) AS is_private,
        SAFE_CAST(JSON_VALUE(p.data_json, '$.fork') AS BOOL) AS is_fork,

        -- Counts
        SAFE_CAST(JSON_VALUE(p.data_json, '$.archived') AS BOOL) AS is_archived,
        SAFE_CAST(JSON_VALUE(p.data_json, '$.disabled') AS BOOL) AS is_disabled,
        JSON_VALUE(p.data_json, '$.description') AS description,
        JSON_VALUE(p.data_json, '$.visibility') AS visibility,
        JSON_VALUE(p.data_json, '$.language') AS primary_language,
        JSON_VALUE(p.data_json, '$.default_branch') AS default_branch,
        SAFE_CAST(JSON_VALUE(p.data_json, '$.size') AS INT64) AS repo_size_kb,

        -- Timestamps
        SAFE_CAST(JSON_VALUE(p.data_json, '$.stargazers_count') AS INT64) AS stargazers_count,
        SAFE_CAST(JSON_VALUE(p.data_json, '$.watchers_count') AS INT64) AS watchers_count,
        SAFE_CAST(JSON_VALUE(p.data_json, '$.forks_count') AS INT64) AS forks_count,

        -- Owner org (prefer org.login if present, fallback to raw org)
        SAFE_CAST(JSON_VALUE(p.data_json, '$.open_issues_count') AS INT64) AS open_issues_count,
        SAFE_CAST(JSON_VALUE(p.data_json, '$.subscribers_count') AS INT64) AS subscribers_count,
        SAFE_CAST(JSON_VALUE(p.data_json, '$.network_count') AS INT64) AS network_count,
        SAFE.PARSE_TIMESTAMP(
            '%Y-%m-%dT%H:%M:%E*S%Ez',
            JSON_VALUE(p.data_json, '$.created_at')
        ) AS created_at,
        SAFE.PARSE_TIMESTAMP(
            '%Y-%m-%dT%H:%M:%E*S%Ez',
            JSON_VALUE(p.data_json, '$.updated_at')
        ) AS updated_at,
        SAFE.PARSE_TIMESTAMP(
            '%Y-%m-%dT%H:%M:%E*S%Ez',
            JSON_VALUE(p.data_json, '$.pushed_at')
        ) AS pushed_at,

        -- Stitch metadata
        JSON_VALUE(p.data_json, '$.owner.login') AS owner_login,
        SAFE_CAST(JSON_VALUE(p.data_json, '$.owner.id') AS INT64) AS owner_id,
        JSON_VALUE(p.data_json, '$.owner.type') AS owner_type,
        JSON_VALUE(p.data_json, '$.owner.html_url') AS owner_github_url,
        JSON_VALUE(p.data_json, '$.owner.avatar_url') AS owner_avatar_url,
        SAFE_CAST(JSON_VALUE(p.data_json, '$.owner.site_admin') AS BOOL) AS owner_is_site_admin
    FROM parsed AS p
),

deduped AS (
    SELECT *
    FROM (
        SELECT
            f.*,
            ROW_NUMBER() OVER (
                PARTITION BY f.repo_id
                ORDER BY
                    COALESCE(f.updated_at, f.pushed_at, f.created_at) DESC,
                    f._sdc_extracted_at DESC
            ) AS row_num
        FROM flattened AS f
    )
    WHERE row_num = 1
),

final AS (
    SELECT
        repo_id,

        -- Canonical repo identifiers
        node_id,
        org,
        full_name,
        -- keep both forms just in case; prefer repo from full_name/name
        repo_github_url,
        git_url,

        -- URLs
        ssh_url,
        clone_url,
        homepage_url,
        is_private,
        is_fork,

        -- Flags
        is_archived,
        is_disabled,
        description,
        visibility,

        -- Metadata
        primary_language,
        default_branch,
        repo_size_kb,
        stargazers_count,

        -- Counts
        watchers_count,
        forks_count,
        open_issues_count,
        subscribers_count,
        network_count,
        created_at,
        updated_at,

        -- Timestamps
        pushed_at,
        owner_login,
        owner_id,

        -- Owner
        owner_type,
        owner_github_url,
        owner_avatar_url,
        _sdc_extracted_at,
        _sdc_received_at,
        _sdc_batched_at,

        -- Derived
        _sdc_deleted_at,

        -- Stitch metadata
        _sdc_sequence,
        _sdc_table_version,
        CONCAT(
            COALESCE(CAST(repo_id AS STRING), ''),
            ':',
            COALESCE(full_name, COALESCE(org, ''), '/', COALESCE(repo, ''))
        ) AS repo_sk,
        COALESCE(repo, repo_slug) AS repo,
        COALESCE(owner_is_site_admin, FALSE) AS owner_is_site_admin,
        CONCAT(org, '/', COALESCE(repo, repo_slug)) AS full_repo_name,

        CURRENT_TIMESTAMP() AS dbt_updated_at
    FROM deduped
)

SELECT *
FROM final
WHERE
    repo_id IS NOT NULL
    AND org IS NOT NULL
    AND COALESCE(repo, '') != ''
