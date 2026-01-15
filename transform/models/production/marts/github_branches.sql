{{ config(
    materialized='table',
    tags=['github', 'production']
) }}

WITH branches AS (
    SELECT
        b.branch_sk,
        b.org,
        b.repo,
        b.repo_id,
        b.branch_name,
        b.is_protected,
        b.commit_sha,
        b.commit_api_url,
        b.full_repo_name,
        b._sdc_extracted_at
    FROM {{ ref('stg_github_branches') }} AS b
),

final AS (
    SELECT
        -- Keys
        branch_sk,

        -- Repo
        org,
        repo,
        repo_id,
        full_repo_name,

        -- Branch
        branch_name,
        is_protected,

        -- Head commit
        commit_sha,
        commit_api_url,

        -- Useful flags
        _sdc_extracted_at,

        -- Metadata
        (branch_name = 'main') AS is_main_branch,
        CURRENT_TIMESTAMP() AS dbt_updated_at
    FROM branches
)

SELECT *
FROM final
