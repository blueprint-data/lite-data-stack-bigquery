{{ config(
    materialized='incremental',
    unique_key='sha',
    tags=['github']
) }}

WITH source_data AS (
    SELECT s.*
    FROM {{ source('tap_github', 'commits') }} AS s
    {% if is_incremental() %}
        WHERE s._sdc_extracted_at > (
            SELECT MAX(t._sdc_extracted_at)
            FROM {{ this }} AS t
        )
    {% endif %}
),

parsed_source AS (
    SELECT
        -- Stitch metadata
        s._sdc_extracted_at,
        s._sdc_received_at,
        s._sdc_batched_at,
        s._sdc_deleted_at,
        s._sdc_sequence,
        s._sdc_table_version,

        -- Raw JSON
        s.data AS data_json,

        -- Common identifiers
        JSON_VALUE(s.data, '$.sha') AS sha,
        SAFE.PARSE_TIMESTAMP(
            '%Y-%m-%dT%H:%M:%E*S%Ez',
            JSON_VALUE(s.data, '$.commit_timestamp')
        ) AS commit_timestamp
    FROM source_data AS s
),

deduped_source AS (
    SELECT *
    FROM (
        SELECT
            sd.*,
            ROW_NUMBER() OVER (
                PARTITION BY sd.sha
                ORDER BY
                    sd.commit_timestamp DESC,
                    sd._sdc_extracted_at DESC
            ) AS row_num
        FROM parsed_source AS sd
    )
    WHERE row_num = 1
),

parsed_commits AS (
    SELECT
        -- Repo identifiers
        sd.sha,
        sd._sdc_extracted_at,
        sd._sdc_received_at,
        sd._sdc_batched_at,

        -- Commit identifiers
        sd._sdc_deleted_at,
        sd._sdc_sequence,
        sd._sdc_table_version,

        -- Stitch metadata
        sd.commit_timestamp,
        JSON_VALUE(sd.data_json, '$.org') AS org,
        JSON_VALUE(sd.data_json, '$.repo') AS repo,
        SAFE_CAST(JSON_VALUE(sd.data_json, '$.repo_id') AS INT64) AS repo_id,
        JSON_VALUE(sd.data_json, '$.node_id') AS node_id,
        JSON_VALUE(sd.data_json, '$.url') AS commit_api_url,

        -- Normalize nested payloads to JSON strings
        JSON_VALUE(sd.data_json, '$.html_url') AS commit_github_url,
        TO_JSON_STRING(JSON_QUERY(sd.data_json, '$.commit')) AS commit_json,
        TO_JSON_STRING(JSON_QUERY(sd.data_json, '$.author')) AS author_json,

        -- Top-level commit timestamp (tap)
        TO_JSON_STRING(JSON_QUERY(sd.data_json, '$.committer')) AS committer_json
    FROM deduped_source AS sd
),

exploded_commits AS (
    SELECT
        pc.org,
        pc.repo,
        pc.repo_id,
        pc.node_id,

        pc.sha,
        pc.commit_api_url,
        pc.commit_github_url,
        pc.commit_timestamp,

        pc._sdc_extracted_at,
        pc._sdc_received_at,
        pc._sdc_batched_at,
        pc._sdc_deleted_at,
        pc._sdc_sequence,
        pc._sdc_table_version,

        -- Commit details
        JSON_VALUE(pc.commit_json, '$.message') AS commit_message,
        SAFE_CAST(JSON_VALUE(pc.commit_json, '$.comment_count') AS INT64) AS comment_count,

        -- Git author (from commit payload)
        JSON_VALUE(pc.commit_json, '$.author.name') AS git_author_name,
        JSON_VALUE(pc.commit_json, '$.author.email') AS git_author_email,
        SAFE.PARSE_TIMESTAMP(
            '%Y-%m-%dT%H:%M:%E*S%Ez',
            JSON_VALUE(pc.commit_json, '$.author.date')
        ) AS git_author_timestamp,

        -- Git committer (from commit payload)
        JSON_VALUE(pc.commit_json, '$.committer.name') AS git_committer_name,
        JSON_VALUE(pc.commit_json, '$.committer.email') AS git_committer_email,
        SAFE.PARSE_TIMESTAMP(
            '%Y-%m-%dT%H:%M:%E*S%Ez',
            JSON_VALUE(pc.commit_json, '$.committer.date')
        ) AS git_committer_timestamp,

        -- Tree info
        JSON_VALUE(pc.commit_json, '$.tree.sha') AS tree_sha,
        JSON_VALUE(pc.commit_json, '$.tree.url') AS tree_url,

        -- Verification
        SAFE_CAST(JSON_VALUE(pc.commit_json, '$.verification.verified') AS BOOL) AS is_verified,
        JSON_VALUE(pc.commit_json, '$.verification.reason') AS verification_reason,
        SAFE.PARSE_TIMESTAMP(
            '%Y-%m-%dT%H:%M:%E*S%Ez',
            JSON_VALUE(pc.commit_json, '$.verification.verified_at')
        ) AS verified_at,

        -- GitHub author profile
        SAFE_CAST(JSON_VALUE(pc.author_json, '$.id') AS INT64) AS github_author_id,
        JSON_VALUE(pc.author_json, '$.login') AS github_author_login,
        JSON_VALUE(pc.author_json, '$.avatar_url') AS github_author_avatar_url,
        JSON_VALUE(pc.author_json, '$.html_url') AS github_author_profile_url,
        JSON_VALUE(pc.author_json, '$.type') AS github_author_type,
        SAFE_CAST(JSON_VALUE(pc.author_json, '$.site_admin') AS BOOL) AS github_author_is_site_admin,

        -- GitHub committer profile
        SAFE_CAST(JSON_VALUE(pc.committer_json, '$.id') AS INT64) AS github_committer_id,
        JSON_VALUE(pc.committer_json, '$.login') AS github_committer_login,
        JSON_VALUE(pc.committer_json, '$.avatar_url') AS github_committer_avatar_url,
        JSON_VALUE(pc.committer_json, '$.html_url') AS github_committer_profile_url,
        JSON_VALUE(pc.committer_json, '$.type') AS github_committer_type,
        SAFE_CAST(JSON_VALUE(pc.committer_json, '$.site_admin') AS BOOL) AS github_committer_is_site_admin
    FROM parsed_commits AS pc
),

final AS (
    SELECT
        ec.org,
        ec.repo,
        ec.repo_id,
        ec.node_id,

        ec.sha,
        ec.commit_api_url,
        ec.commit_github_url,
        ec.commit_timestamp,

        ec.commit_message,
        ec.comment_count,

        ec.git_author_name,
        ec.git_author_email,
        ec.git_author_timestamp,

        ec.git_committer_name,
        ec.git_committer_email,
        ec.git_committer_timestamp,

        ec.tree_sha,
        ec.tree_url,

        ec.is_verified,
        ec.verification_reason,
        ec.verified_at,

        ec.github_author_id,
        ec.github_author_login,
        ec.github_author_avatar_url,
        ec.github_author_profile_url,
        ec.github_author_type,
        ec.github_author_is_site_admin,

        ec.github_committer_id,
        ec.github_committer_login,
        ec.github_committer_avatar_url,
        ec.github_committer_profile_url,
        ec.github_committer_type,
        ec.github_committer_is_site_admin,

        ec._sdc_extracted_at,
        ec._sdc_received_at,
        ec._sdc_batched_at,
        ec._sdc_deleted_at,
        ec._sdc_sequence,
        ec._sdc_table_version,

        -- Derived (lightweight, ok for staging)
        SAFE_CAST(REGEXP_EXTRACT(ec.commit_message, '#([0-9]+)') AS INT64)
            AS pull_request_number,

        DATE(ec.commit_timestamp) AS commit_date,
        DATE_TRUNC(DATE(ec.commit_timestamp), WEEK (MONDAY)) AS commit_week,
        DATE_TRUNC(DATE(ec.commit_timestamp), MONTH) AS commit_month,
        DATE_TRUNC(DATE(ec.commit_timestamp), QUARTER) AS commit_quarter,
        DATE_TRUNC(DATE(ec.commit_timestamp), YEAR) AS commit_year,
        EXTRACT(DAYOFWEEK FROM ec.commit_timestamp) AS commit_day_of_week,
        EXTRACT(HOUR FROM ec.commit_timestamp) AS commit_hour,
        (
            ec.git_author_email IS NOT NULL
            AND ec.git_committer_email IS NOT NULL
            AND ec.git_author_email != ec.git_committer_email
        ) AS is_merge_commit,
        CASE
            WHEN REGEXP_CONTAINS(LOWER(ec.commit_message), '^feat(\\(|:)\\b') THEN 'feature'
            WHEN REGEXP_CONTAINS(LOWER(ec.commit_message), '^fix(\\(|:)\\b') THEN 'fix'
            WHEN REGEXP_CONTAINS(LOWER(ec.commit_message), '^docs(\\(|:)\\b') THEN 'docs'
            WHEN REGEXP_CONTAINS(LOWER(ec.commit_message), '^refactor(\\(|:)\\b') THEN 'refactor'
            WHEN REGEXP_CONTAINS(LOWER(ec.commit_message), '^test(\\(|:)\\b') THEN 'test'
            WHEN REGEXP_CONTAINS(LOWER(ec.commit_message), '^chore(\\(|:)\\b') THEN 'chore'
            WHEN REGEXP_CONTAINS(LOWER(ec.commit_message), '^perf(\\(|:)\\b') THEN 'performance'
            WHEN REGEXP_CONTAINS(LOWER(ec.commit_message), '^style(\\(|:)\\b') THEN 'style'
            WHEN REGEXP_CONTAINS(LOWER(ec.commit_message), '^ci(\\(|:)\\b') THEN 'ci'
            WHEN REGEXP_CONTAINS(LOWER(ec.commit_message), '^build(\\(|:)\\b') THEN 'build'
            ELSE 'other'
        END AS commit_type
    FROM exploded_commits AS ec
)

SELECT *
FROM final
WHERE sha IS NOT NULL
