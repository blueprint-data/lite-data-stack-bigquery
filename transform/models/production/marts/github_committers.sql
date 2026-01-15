{{ config(
    materialized='table',
    tags=['github', 'production']
) }}

WITH commits AS (
    SELECT *
    FROM {{ ref('stg_github_commits') }}
    WHERE github_author_id IS NOT NULL
),

author_email_mode AS (
    SELECT
        github_author_id,
        git_author_email,
        email_count
    FROM (
        SELECT
            github_author_id,
            git_author_email,
            email_count,
            ROW_NUMBER() OVER (
                PARTITION BY github_author_id
                ORDER BY email_count DESC, git_author_email ASC
            ) AS email_rank
        FROM (
            SELECT
                github_author_id,
                git_author_email,
                COUNT(*) AS email_count
            FROM commits
            WHERE git_author_email IS NOT NULL
            GROUP BY github_author_id, git_author_email
        ) AS email_counts
    ) AS ranked_emails
    WHERE email_rank = 1
),

/*
  Elegimos valores representativos del perfil (login, avatar, etc.)
  usando el commit más reciente por autor (evita ANY_VALUE arbitrario).
*/
author_profile_latest AS (
    SELECT
        github_author_id,
        github_author_login,
        github_author_avatar_url,
        github_author_profile_url,
        github_author_type,
        git_author_name,
        commit_timestamp,
        COALESCE(github_author_is_site_admin, FALSE) AS github_author_is_site_admin
    FROM (
        SELECT
            github_author_id,
            github_author_login,
            github_author_avatar_url,
            github_author_profile_url,
            github_author_type,
            git_author_name,
            commit_timestamp,
            github_author_is_site_admin,
            ROW_NUMBER() OVER (
                PARTITION BY github_author_id
                ORDER BY commit_timestamp DESC
            ) AS profile_rank
        FROM commits
    ) AS ranked_profiles
    WHERE profile_rank = 1
),

committer_base_stats AS (
    SELECT
        c.github_author_id,

        -- Perfil (del commit más reciente)
        p.github_author_login,
        p.github_author_avatar_url,
        p.github_author_profile_url,
        p.github_author_type,
        p.git_author_name,

        -- Email principal (mode)
        aem.git_author_email,

        -- Site admin (por si varía)
        MAX(COALESCE(c.github_author_is_site_admin, FALSE)) AS is_site_admin,

        -- Conteos base
        COUNT(*) AS total_commits,
        COUNT(DISTINCT c.repo) AS repos_contributed_to,
        COUNT(DISTINCT c.commit_date) AS active_days,

        -- Actividad temporal
        MIN(c.commit_timestamp) AS first_commit_at,
        MAX(c.commit_timestamp) AS last_commit_at,
        DIV(
            UNIX_SECONDS(MAX(c.commit_timestamp)) - UNIX_SECONDS(MIN(c.commit_timestamp)),
            86400
        ) AS days_active_span,

        -- Breakdown por tipo
        COUNTIF(c.commit_type = 'fix') AS fix_commits,
        COUNTIF(c.commit_type = 'feature') AS feature_commits,
        COUNTIF(c.commit_type = 'docs') AS docs_commits,
        COUNTIF(c.commit_type = 'refactor') AS refactor_commits,
        COUNTIF(c.commit_type = 'test') AS test_commits,
        COUNTIF(c.commit_type = 'chore') AS chore_commits,
        COUNTIF(c.commit_type = 'performance') AS performance_commits,
        COUNTIF(c.commit_type = 'other') AS other_commits,

        -- Verificación (null-safe)
        COUNTIF(COALESCE(c.is_verified, FALSE)) AS verified_commits,
        COUNTIF(NOT COALESCE(c.is_verified, FALSE)) AS unverified_commits,

        -- Merge commits (null-safe)
        COUNTIF(COALESCE(c.is_merge_commit, FALSE)) AS merge_commits,

        -- PRs
        COUNTIF(c.pull_request_number IS NOT NULL) AS pr_commits,
        COUNT(DISTINCT c.pull_request_number) AS unique_prs

    FROM commits AS c
    LEFT JOIN author_profile_latest AS p
        ON c.github_author_id = p.github_author_id
    LEFT JOIN author_email_mode AS aem
        ON c.github_author_id = aem.github_author_id
    GROUP BY
        c.github_author_id,
        p.github_author_login,
        p.github_author_avatar_url,
        p.github_author_profile_url,
        p.github_author_type,
        p.git_author_name,
        aem.git_author_email
),

recent_activity AS (
    SELECT
        github_author_id,

        COUNTIF(
            commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
        ) AS commits_last_30_days,
        COUNTIF(
            commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
        ) AS commits_last_90_days,
        COUNTIF(
            commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
        ) AS commits_last_180_days,
        COUNTIF(
            commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
        ) AS commits_last_365_days,

        COUNT(DISTINCT IF(
            commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY),
            commit_date,
            NULL
        )) AS active_days_last_30,
        COUNT(DISTINCT IF(
            commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY),
            commit_date,
            NULL
        )) AS active_days_last_90,
        COUNT(DISTINCT IF(
            commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY),
            commit_date,
            NULL
        )) AS active_days_last_365,

        MAX(commit_timestamp) AS most_recent_commit_at
    FROM commits
    GROUP BY github_author_id
),

time_pattern_counts AS (
    SELECT
        github_author_id,

        -- BigQuery: 1=Sunday, 7=Saturday (como ya venís usando)
        COUNTIF(commit_day_of_week IN (1, 7)) AS weekend_commits,
        COUNTIF(commit_day_of_week BETWEEN 2 AND 6) AS weekday_commits,

        COUNTIF(commit_hour BETWEEN 9 AND 17) AS business_hours_commits,
        COUNTIF(commit_hour < 9 OR commit_hour > 17) AS off_hours_commits
    FROM commits
    GROUP BY github_author_id
),

hour_counts AS (
    SELECT
        github_author_id,
        commit_hour,
        COUNT(*) AS commit_hour_count
    FROM commits
    GROUP BY github_author_id, commit_hour
),

hour_mode AS (
    SELECT
        github_author_id,
        most_active_hour
    FROM (
        SELECT
            github_author_id,
            commit_hour AS most_active_hour,
            ROW_NUMBER() OVER (
                PARTITION BY github_author_id
                ORDER BY commit_hour_count DESC, commit_hour ASC
            ) AS hour_rank
        FROM hour_counts
    ) AS ranked_hours
    WHERE hour_rank = 1
),

dow_counts AS (
    SELECT
        github_author_id,
        commit_day_of_week,
        COUNT(*) AS commit_dow_count
    FROM commits
    GROUP BY github_author_id, commit_day_of_week
),

dow_mode AS (
    SELECT
        github_author_id,
        most_active_day_of_week
    FROM (
        SELECT
            github_author_id,
            commit_day_of_week AS most_active_day_of_week,
            ROW_NUMBER() OVER (
                PARTITION BY github_author_id
                ORDER BY commit_dow_count DESC, commit_day_of_week ASC
            ) AS dow_rank
        FROM dow_counts
    ) AS ranked_days
    WHERE dow_rank = 1
),

time_patterns AS (
    SELECT
        c.github_author_id,
        c.weekend_commits,
        c.weekday_commits,
        c.business_hours_commits,
        c.off_hours_commits,
        hm.most_active_hour,
        dm.most_active_day_of_week
    FROM time_pattern_counts AS c
    LEFT JOIN hour_mode AS hm
        ON c.github_author_id = hm.github_author_id
    LEFT JOIN dow_mode AS dm
        ON c.github_author_id = dm.github_author_id
),

contribution_metrics AS (
    SELECT
        github_author_id,
        AVG(commits_per_day) AS avg_commits_per_active_day,
        MAX(commits_per_day) AS max_commits_in_day
    FROM (
        SELECT
            github_author_id,
            commit_date,
            COUNT(*) AS commits_per_day
        FROM commits
        GROUP BY github_author_id, commit_date
    )
    GROUP BY github_author_id
),

final AS (
    SELECT
        -- Identity
        base.github_author_id AS committer_id,
        base.github_author_login AS github_login,
        base.git_author_name AS committer_name,
        base.git_author_email AS committer_email,

        -- Profile
        base.github_author_avatar_url AS avatar_url,
        base.github_author_profile_url AS profile_url,
        base.github_author_type AS account_type,
        base.is_site_admin,

        -- Overall stats
        base.total_commits,
        base.repos_contributed_to,
        base.active_days,
        base.days_active_span,

        -- First & last
        base.first_commit_at,
        base.last_commit_at,

        -- Type breakdown
        base.fix_commits,
        base.feature_commits,
        base.docs_commits,
        base.refactor_commits,
        base.test_commits,
        base.chore_commits,
        base.performance_commits,
        base.other_commits,

        -- Verification
        base.verified_commits,
        base.unverified_commits,

        -- Merge/PR
        base.merge_commits,
        base.pr_commits,
        base.unique_prs,

        -- Recent
        recent.commits_last_30_days,
        recent.commits_last_90_days,
        recent.commits_last_180_days,
        recent.commits_last_365_days,

        recent.active_days_last_30,
        recent.active_days_last_90,
        recent.active_days_last_365,

        -- Patterns
        time_p.weekend_commits,
        time_p.weekday_commits,
        time_p.business_hours_commits,
        time_p.off_hours_commits,
        time_p.most_active_hour,
        time_p.most_active_day_of_week,

        -- Contribution intensity
        contrib.avg_commits_per_active_day,
        contrib.max_commits_in_day,

        -- Derived metrics
        DIV(
            UNIX_SECONDS(CURRENT_TIMESTAMP()) - UNIX_SECONDS(base.last_commit_at),
            86400
        ) AS days_since_last_commit,

        CASE
            WHEN recent.commits_last_30_days > 0 THEN 'Very Active'
            WHEN recent.commits_last_90_days > 0 THEN 'Active'
            WHEN recent.commits_last_180_days > 0 THEN 'Moderately Active'
            WHEN recent.commits_last_365_days > 0 THEN 'Less Active'
            ELSE 'Inactive'
        END AS activity_status,

        ROUND(100.0 * SAFE_DIVIDE(base.fix_commits, base.total_commits), 2) AS fix_commits_pct,
        ROUND(100.0 * SAFE_DIVIDE(base.feature_commits, base.total_commits), 2) AS feature_commits_pct,
        ROUND(100.0 * SAFE_DIVIDE(base.docs_commits, base.total_commits), 2) AS docs_commits_pct,
        ROUND(100.0 * SAFE_DIVIDE(base.refactor_commits, base.total_commits), 2) AS refactor_commits_pct,
        ROUND(100.0 * SAFE_DIVIDE(base.verified_commits, base.total_commits), 2) AS verification_rate_pct,

        ROUND(100.0 * SAFE_DIVIDE(base.merge_commits, base.total_commits), 2) AS merge_commit_pct,
        ROUND(SAFE_DIVIDE(base.total_commits, base.active_days), 2) AS commits_per_active_day,
        ROUND(
            SAFE_DIVIDE(base.total_commits, GREATEST(base.days_active_span, 1)),
            2
        ) AS avg_commits_per_day_in_span,

        ROW_NUMBER() OVER (ORDER BY base.total_commits DESC) AS rank_by_total_commits,
        ROW_NUMBER() OVER (ORDER BY recent.commits_last_90_days DESC) AS rank_by_recent_activity,

        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM committer_base_stats AS base
    LEFT JOIN recent_activity AS recent
        ON base.github_author_id = recent.github_author_id
    LEFT JOIN time_patterns AS time_p
        ON base.github_author_id = time_p.github_author_id
    LEFT JOIN contribution_metrics AS contrib
        ON base.github_author_id = contrib.github_author_id
)

SELECT *
FROM final
ORDER BY total_commits DESC
