{{ config(
    materialized='table',
    tags=['github', 'production']
) }}

SELECT
    pull_request_commit_sk,

    org,
    repo,
    repo_id,
    full_repo_name,

    pull_request_number,

    sha,
    node_id,
    commit_api_url,
    commit_github_url,
    comments_api_url,

    commit_message,
    comment_count,

    commit_type,
    is_merge_commit,

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

    commit_date,
    commit_week,
    commit_month,
    commit_quarter,
    commit_year,
    commit_day_of_week,
    commit_hour,

    _sdc_extracted_at,
    dbt_updated_at
FROM {{ ref('stg_github_pull_requests_commits') }}
