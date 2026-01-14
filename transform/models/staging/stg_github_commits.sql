{{ config(
    materialized='incremental',
    unique_key='sha',
    tags=['github']
) }}

WITH source_data AS (
    SELECT commits_source.*
    FROM {{ source('metabase_github', 'commits') }} AS commits_source
    {% if is_incremental() %}
        WHERE
            SAFE_CAST(commits_source.commit_timestamp AS TIMESTAMP)
            > (SELECT MAX(this.commit_timestamp) FROM {{ this }} AS this)
    {% endif %}
),

parsed_commits AS (
    SELECT
        -- Repository identifiers
        org,
        repo,
        repo_id,
        node_id,

        -- Commit identifiers
        sha,
        url AS commit_api_url,
        html_url AS commit_github_url,
        SAFE_CAST(commit_timestamp AS TIMESTAMP) AS commit_timestamp,

        -- Parse COMMIT JSON object
        _sdc_extracted_at,

        -- Parse AUTHOR JSON object
        _sdc_received_at,

        -- Parse COMMITTER JSON object
        _sdc_batched_at,

        -- Stitch metadata
        _sdc_deleted_at,
        _sdc_sequence,
        _sdc_table_version,
        _sdc_sync_started_at,
        commit,
        author,
        committer
    FROM source_data
),

exploded_commits AS (
    SELECT
        -- Repository identifiers
        org,
        repo,
        repo_id,
        node_id,

        -- Commit identifiers
        sha,
        commit_api_url,
        commit_github_url,
        commit_timestamp,

        -- Commit details from COMMIT JSON
        commit.message AS commit_message,
        SAFE_CAST(commit.comment_count AS INT64) AS comment_count,

        -- Author details from COMMIT JSON (git author)
        commit.author.name AS git_author_name,
        commit.author.email AS git_author_email,
        commit.committer.name AS git_committer_name,

        -- Committer details from COMMIT JSON (git committer)
        commit.committer.email AS git_committer_email,
        commit.tree.sha AS tree_sha,
        commit.tree.url AS tree_url,

        -- Tree information
        SAFE_CAST(commit.verification.verified AS BOOL) AS is_verified,
        commit.verification.reason AS verification_reason,

        -- Verification details
        SAFE_CAST(author.id AS INT64) AS github_author_id,
        author.login AS github_author_login,
        author.avatar_url AS github_author_avatar_url,

        -- GitHub author details from AUTHOR JSON
        author.html_url AS github_author_profile_url,
        author.type AS github_author_type,
        SAFE_CAST(author.site_admin AS BOOL) AS github_author_is_site_admin,
        SAFE_CAST(committer.id AS INT64) AS github_committer_id,
        committer.login AS github_committer_login,
        committer.avatar_url AS github_committer_avatar_url,

        -- GitHub committer details from COMMITTER JSON
        committer.html_url AS github_committer_profile_url,
        committer.type AS github_committer_type,
        SAFE_CAST(committer.site_admin AS BOOL) AS github_committer_is_site_admin,
        _sdc_extracted_at,
        _sdc_received_at,
        _sdc_batched_at,

        -- Stitch metadata
        _sdc_deleted_at,
        _sdc_sequence,
        _sdc_table_version,
        _sdc_sync_started_at,
        SAFE_CAST(commit.author.date AS TIMESTAMP) AS git_author_timestamp,
        SAFE_CAST(commit.committer.date AS TIMESTAMP) AS git_committer_timestamp,
        SAFE_CAST(commit.verification.verified_at AS TIMESTAMP) AS verified_at
    FROM parsed_commits
),

final AS (
    SELECT
        -- Repository identifiers
        org,
        repo,
        repo_id,
        node_id,

        -- Commit identifiers
        sha,
        commit_api_url,
        commit_github_url,
        commit_timestamp,

        -- Commit details
        commit_message,
        comment_count,

        -- Extract PR number from commit message (pattern: #12345)
        SAFE_CAST(REGEXP_EXTRACT(commit_message, r'#([0-9]+)') AS INT64) AS pull_request_number,

        -- Extract commit type from message (e.g., "fix:", "feat:", "docs:")
        git_author_name,

        -- Git author details
        git_author_email,
        git_author_timestamp,
        git_committer_name,

        -- Git committer details
        git_committer_email,
        git_committer_timestamp,
        tree_sha,

        -- Check if author and committer are different
        tree_url,

        -- Tree information
        is_verified,
        verification_reason,

        -- Verification details
        verified_at,
        github_author_id,
        github_author_login,

        -- GitHub author details
        github_author_avatar_url,
        github_author_profile_url,
        github_author_type,
        github_author_is_site_admin,
        github_committer_id,
        github_committer_login,

        -- GitHub committer details
        github_committer_avatar_url,
        github_committer_profile_url,
        github_committer_type,
        github_committer_is_site_admin,
        _sdc_extracted_at,
        _sdc_received_at,

        -- Date dimensions for analysis
        _sdc_batched_at,
        _sdc_deleted_at,
        _sdc_sequence,
        _sdc_table_version,
        _sdc_sync_started_at,
        CASE
            WHEN LOWER(commit_message) LIKE 'feat:%' OR LOWER(commit_message) LIKE 'feat(%' THEN 'feature'
            WHEN LOWER(commit_message) LIKE 'fix:%' OR LOWER(commit_message) LIKE 'fix(%' THEN 'fix'
            WHEN LOWER(commit_message) LIKE 'docs:%' OR LOWER(commit_message) LIKE 'docs(%' THEN 'docs'
            WHEN
                LOWER(commit_message) LIKE 'refactor:%' OR LOWER(commit_message) LIKE 'refactor(%'
                THEN 'refactor'
            WHEN LOWER(commit_message) LIKE 'test:%' OR LOWER(commit_message) LIKE 'test(%' THEN 'test'
            WHEN LOWER(commit_message) LIKE 'chore:%' OR LOWER(commit_message) LIKE 'chore(%' THEN 'chore'
            WHEN LOWER(commit_message) LIKE 'perf:%' OR LOWER(commit_message) LIKE 'perf(%' THEN 'performance'
            WHEN LOWER(commit_message) LIKE 'style:%' OR LOWER(commit_message) LIKE 'style(%' THEN 'style'
            WHEN LOWER(commit_message) LIKE 'ci:%' OR LOWER(commit_message) LIKE 'ci(%' THEN 'ci'
            WHEN LOWER(commit_message) LIKE 'build:%' OR LOWER(commit_message) LIKE 'build(%' THEN 'build'
            ELSE 'other'
        END AS commit_type,
        git_author_email != git_committer_email AS is_merge_commit,

        -- Stitch metadata
        DATE(commit_timestamp) AS commit_date,
        DATE_TRUNC(DATE(commit_timestamp), WEEK(MONDAY)) AS commit_week,
        DATE_TRUNC(DATE(commit_timestamp), MONTH) AS commit_month,
        DATE_TRUNC(DATE(commit_timestamp), QUARTER) AS commit_quarter,
        DATE_TRUNC(DATE(commit_timestamp), YEAR) AS commit_year,
        EXTRACT(DAYOFWEEK FROM commit_timestamp) AS commit_day_of_week,
        EXTRACT(HOUR FROM commit_timestamp) AS commit_hour
    FROM exploded_commits
)

SELECT * FROM final
