{{ config(
    materialized='incremental',
    unique_key='sha',
    tags=['github']
) }}

WITH source_data AS (
    SELECT s.*
    FROM {{ source('metabase_github', 'commits') }} AS s
    {% if is_incremental() %}
        WHERE s._sdc_extracted_at > (
            SELECT MAX(t._sdc_extracted_at)
            FROM {{ this }} AS t
        )
    {% endif %}
),

parsed_commits AS (
    SELECT
        -- Repo identifiers
        sd.org,
        sd.repo,
        sd.repo_id,
        sd.node_id,

        -- Commit identifiers
        sd.sha,
        sd.url AS commit_api_url,
        sd.html_url AS commit_github_url,

        -- Stitch metadata
        sd._sdc_extracted_at,
        sd._sdc_received_at,
        sd._sdc_batched_at,
        sd._sdc_deleted_at,
        sd._sdc_sequence,
        sd._sdc_table_version,

        -- Normalize nested payloads to JSON strings (works whether original is STRUCT or already JSON-like)
        TO_JSON_STRING(sd.commit) AS commit_json,
        TO_JSON_STRING(sd.author) AS author_json,
        TO_JSON_STRING(sd.committer) AS committer_json,

        -- Commit timestamp (top-level from tap)
        SAFE_CAST(sd.commit_timestamp AS TIMESTAMP) AS commit_timestamp
    FROM source_data AS sd
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

        -- PR number from message (safe raw string)
        ec.git_author_name,

        -- Conventional commit classification
        ec.git_author_email,

        -- Merge-ish heuristic (author != committer)
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

        -- Derived fields
        SAFE_CAST(REGEXP_EXTRACT(ec.commit_message, '#([0-9]+)') AS INT64)
            AS pull_request_number,
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
        END AS commit_type,
        (
            ec.git_author_email IS NOT NULL
            AND ec.git_committer_email IS NOT NULL
            AND ec.git_author_email != ec.git_committer_email
        ) AS is_merge_commit,

        DATE(ec.commit_timestamp) AS commit_date,
        DATE_TRUNC(DATE(ec.commit_timestamp), WEEK (MONDAY)) AS commit_week,
        DATE_TRUNC(DATE(ec.commit_timestamp), MONTH) AS commit_month,
        DATE_TRUNC(DATE(ec.commit_timestamp), QUARTER) AS commit_quarter,
        DATE_TRUNC(DATE(ec.commit_timestamp), YEAR) AS commit_year,
        EXTRACT(DAYOFWEEK FROM ec.commit_timestamp) AS commit_day_of_week,
        EXTRACT(HOUR FROM ec.commit_timestamp) AS commit_hour
    FROM exploded_commits AS ec
)

SELECT *
FROM final
WHERE sha IS NOT NULL
