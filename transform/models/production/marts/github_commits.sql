{{ config(
    materialized='incremental',
    unique_key='sha',
    tags=['github', 'production']
) }}

WITH commits AS (
    SELECT stg.*
    FROM {{ ref('stg_github_commits') }} AS stg
    {% if is_incremental() %}
        WHERE stg.commit_timestamp > (SELECT MAX(this.commit_timestamp) FROM {{ this }} AS this)
    {% endif %}
)

SELECT
    -- Primary Key
    sha,

    -- Repository Information
    org,
    repo,
    repo_id,
    node_id,
    commit_api_url,

    -- Commit Identifiers & URLs
    commit_github_url,
    commit_timestamp,
    commit_message,

    -- Commit Content
    comment_count,
    pull_request_number,
    commit_type,
    git_author_name,

    -- Git Author Information
    git_author_email,
    git_author_timestamp,
    git_committer_name,

    -- Git Committer Information
    git_committer_email,
    git_committer_timestamp,
    is_merge_commit,

    -- Commit Characteristics
    is_verified,
    verification_reason,
    verified_at,
    tree_sha,

    -- Tree Information
    tree_url,
    github_author_id,

    -- GitHub Author Profile
    github_author_login,
    github_author_avatar_url,
    github_author_profile_url,
    github_author_type,
    github_author_is_site_admin,
    github_committer_id,

    -- GitHub Committer Profile
    github_committer_login,
    github_committer_avatar_url,
    github_committer_profile_url,
    github_committer_type,
    github_committer_is_site_admin,
    commit_date,

    -- Time Dimensions
    commit_week,
    commit_month,
    commit_quarter,
    commit_year,
    commit_day_of_week,
    commit_hour,
    _sdc_extracted_at,

    -- Metadata
    _sdc_received_at,
    _sdc_batched_at,
    _sdc_sequence,
    CONCAT(org, '/', repo) AS full_repo_name,
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM commits