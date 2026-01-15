{{ config(
    materialized='table',
    tags=['github', 'production']
) }}

SELECT
    repo_sk,

    repo_id,
    node_id,

    org,
    repo,
    full_name,
    full_repo_name,

    repo_github_url,
    git_url,
    ssh_url,
    clone_url,
    homepage_url,

    is_private,
    is_fork,
    is_archived,
    is_disabled,

    description,
    visibility,
    primary_language,
    default_branch,

    repo_size_kb,
    stargazers_count,
    watchers_count,
    forks_count,
    open_issues_count,
    subscribers_count,
    network_count,

    created_at,
    updated_at,
    pushed_at,

    owner_login,
    owner_id,
    owner_type,
    owner_github_url,
    owner_avatar_url,
    owner_is_site_admin,

    _sdc_extracted_at,
    dbt_updated_at
FROM {{ ref('stg_github_repositories') }}
