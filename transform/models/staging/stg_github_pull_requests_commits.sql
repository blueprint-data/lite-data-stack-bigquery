{{ config(
    materialized='incremental',
    unique_key='pull_request_commit_sk',
    tags=['github']
) }}

WITH source_data AS (
    SELECT s.*
    FROM {{ source('tap_github', 'pull_request_commits') }} AS s
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

exploded AS (
    SELECT
        -- Repo identifiers (top-level fields inside data)
        p._sdc_extracted_at,
        p._sdc_received_at,
        p._sdc_batched_at,

        -- PR context
        p._sdc_deleted_at,

        -- Commit identifiers
        p._sdc_sequence,
        p._sdc_table_version,
        JSON_VALUE(p.data_json, '$.org') AS org,
        JSON_VALUE(p.data_json, '$.repo') AS repo,
        SAFE_CAST(JSON_VALUE(p.data_json, '$.repo_id') AS INT64) AS repo_id,

        -- Normalize nested payloads to JSON strings
        SAFE_CAST(JSON_VALUE(p.data_json, '$.pull_number') AS INT64) AS pull_request_number,
        JSON_VALUE(p.data_json, '$.sha') AS sha,
        JSON_VALUE(p.data_json, '$.node_id') AS node_id,

        -- Stitch metadata
        JSON_VALUE(p.data_json, '$.url') AS commit_api_url,
        JSON_VALUE(p.data_json, '$.html_url') AS commit_github_url,
        JSON_VALUE(p.data_json, '$.comments_url') AS comments_api_url,
        TO_JSON_STRING(JSON_QUERY(p.data_json, '$.commit')) AS commit_json,
        TO_JSON_STRING(JSON_QUERY(p.data_json, '$.author')) AS author_json,
        TO_JSON_STRING(JSON_QUERY(p.data_json, '$.committer')) AS committer_json
    FROM parsed AS p
),

flattened AS (
    SELECT
        e.org,
        e.repo,
        e.repo_id,
        e.pull_request_number,

        e.sha,
        e.node_id,
        e.commit_api_url,
        e.commit_github_url,
        e.comments_api_url,

        -- Commit details
        e._sdc_extracted_at,
        e._sdc_received_at,

        -- Git author (from commit payload)
        e._sdc_batched_at,
        e._sdc_deleted_at,
        e._sdc_sequence,

        -- Git committer (from commit payload)
        e._sdc_table_version,
        JSON_VALUE(e.commit_json, '$.message') AS commit_message,
        SAFE_CAST(JSON_VALUE(e.commit_json, '$.comment_count') AS INT64) AS comment_count,

        -- Tree info
        JSON_VALUE(e.commit_json, '$.author.name') AS git_author_name,
        JSON_VALUE(e.commit_json, '$.author.email') AS git_author_email,

        -- Verification
        SAFE.PARSE_TIMESTAMP(
            '%Y-%m-%dT%H:%M:%E*S%Ez',
            JSON_VALUE(e.commit_json, '$.author.date')
        ) AS git_author_timestamp,
        JSON_VALUE(e.commit_json, '$.committer.name') AS git_committer_name,
        JSON_VALUE(e.commit_json, '$.committer.email') AS git_committer_email,

        -- GitHub author profile
        SAFE.PARSE_TIMESTAMP(
            '%Y-%m-%dT%H:%M:%E*S%Ez',
            JSON_VALUE(e.commit_json, '$.committer.date')
        ) AS git_committer_timestamp,
        JSON_VALUE(e.commit_json, '$.tree.sha') AS tree_sha,
        JSON_VALUE(e.commit_json, '$.tree.url') AS tree_url,
        SAFE_CAST(JSON_VALUE(e.commit_json, '$.verification.verified') AS BOOL) AS is_verified,
        JSON_VALUE(e.commit_json, '$.verification.reason') AS verification_reason,
        SAFE.PARSE_TIMESTAMP(
            '%Y-%m-%dT%H:%M:%E*S%Ez',
            JSON_VALUE(e.commit_json, '$.verification.verified_at')
        ) AS verified_at,

        -- GitHub committer profile
        SAFE_CAST(JSON_VALUE(e.author_json, '$.id') AS INT64) AS github_author_id,
        JSON_VALUE(e.author_json, '$.login') AS github_author_login,
        JSON_VALUE(e.author_json, '$.avatar_url') AS github_author_avatar_url,
        JSON_VALUE(e.author_json, '$.html_url') AS github_author_profile_url,
        JSON_VALUE(e.author_json, '$.type') AS github_author_type,
        SAFE_CAST(JSON_VALUE(e.author_json, '$.site_admin') AS BOOL) AS github_author_is_site_admin,

        -- Stitch metadata
        SAFE_CAST(JSON_VALUE(e.committer_json, '$.id') AS INT64) AS github_committer_id,
        JSON_VALUE(e.committer_json, '$.login') AS github_committer_login,
        JSON_VALUE(e.committer_json, '$.avatar_url') AS github_committer_avatar_url,
        JSON_VALUE(e.committer_json, '$.html_url') AS github_committer_profile_url,
        JSON_VALUE(e.committer_json, '$.type') AS github_committer_type,
        SAFE_CAST(JSON_VALUE(e.committer_json, '$.site_admin') AS BOOL) AS github_committer_is_site_admin
    FROM exploded AS e
),

deduped AS (
    SELECT *
    FROM (
        SELECT
            f.*,
            ROW_NUMBER() OVER (
                PARTITION BY
                    f.org,
                    f.repo,
                    f.repo_id,
                    f.pull_request_number,
                    f.sha
                ORDER BY
                    f._sdc_extracted_at DESC
            ) AS row_num
        FROM flattened AS f
    )
    WHERE row_num = 1
),

final AS (
    SELECT
        org,

        repo,
        repo_id,
        pull_request_number,
        sha,

        node_id,

        commit_api_url,
        commit_github_url,
        comments_api_url,
        commit_message,
        comment_count,

        git_author_name,
        git_author_email,

        git_author_timestamp,
        git_committer_name,
        git_committer_email,

        git_committer_timestamp,
        tree_sha,
        tree_url,

        is_verified,
        verification_reason,

        verified_at,
        github_author_id,
        github_author_login,

        github_author_avatar_url,
        github_author_profile_url,
        github_author_type,
        github_author_is_site_admin,
        github_committer_id,
        github_committer_login,

        github_committer_avatar_url,
        github_committer_profile_url,
        github_committer_type,
        github_committer_is_site_admin,
        _sdc_extracted_at,
        _sdc_received_at,

        -- Derived fields
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
            ':pr:',
            COALESCE(CAST(pull_request_number AS STRING), ''),
            ':',
            COALESCE(sha, '')
        ) AS pull_request_commit_sk,
        CONCAT(org, '/', repo) AS full_repo_name,
        DATE(git_author_timestamp) AS commit_date,

        DATE_TRUNC(DATE(git_author_timestamp), WEEK (MONDAY)) AS commit_week,

        DATE_TRUNC(DATE(git_author_timestamp), MONTH) AS commit_month,

        DATE_TRUNC(DATE(git_author_timestamp), QUARTER) AS commit_quarter,
        DATE_TRUNC(DATE(git_author_timestamp), YEAR) AS commit_year,
        EXTRACT(DAYOFWEEK FROM git_author_timestamp) AS commit_day_of_week,
        EXTRACT(HOUR FROM git_author_timestamp) AS commit_hour,
        (
            git_author_email IS NOT NULL
            AND git_committer_email IS NOT NULL
            AND git_author_email != git_committer_email
        ) AS is_merge_commit,
        CASE
            WHEN REGEXP_CONTAINS(LOWER(commit_message), '^feat(\\(|:)\\b') THEN 'feature'
            WHEN REGEXP_CONTAINS(LOWER(commit_message), '^fix(\\(|:)\\b') THEN 'fix'
            WHEN REGEXP_CONTAINS(LOWER(commit_message), '^docs(\\(|:)\\b') THEN 'docs'
            WHEN REGEXP_CONTAINS(LOWER(commit_message), '^refactor(\\(|:)\\b') THEN 'refactor'
            WHEN REGEXP_CONTAINS(LOWER(commit_message), '^test(\\(|:)\\b') THEN 'test'
            WHEN REGEXP_CONTAINS(LOWER(commit_message), '^chore(\\(|:)\\b') THEN 'chore'
            WHEN REGEXP_CONTAINS(LOWER(commit_message), '^perf(\\(|:)\\b') THEN 'performance'
            WHEN REGEXP_CONTAINS(LOWER(commit_message), '^style(\\(|:)\\b') THEN 'style'
            WHEN REGEXP_CONTAINS(LOWER(commit_message), '^ci(\\(|:)\\b') THEN 'ci'
            WHEN REGEXP_CONTAINS(LOWER(commit_message), '^build(\\(|:)\\b') THEN 'build'
            ELSE 'other'
        END AS commit_type,

        CURRENT_TIMESTAMP() AS dbt_updated_at
    FROM deduped
)

SELECT *
FROM final
WHERE
    org IS NOT NULL
    AND repo IS NOT NULL
    AND repo_id IS NOT NULL
    AND pull_request_number IS NOT NULL
    AND sha IS NOT NULL
