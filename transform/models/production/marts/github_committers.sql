{{ config(
    materialized='table',
    tags=['github', 'production']
) }}

WITH commits AS (
    SELECT *
    FROM {{ ref('github_commits') }}
),

committer_base_stats AS (
    SELECT
        github_author_id,
        github_author_login,
        github_author_avatar_url,
        github_author_profile_url,
        github_author_type,
        git_author_name,

        -- Git identity information
        git_author_email,
        MAX(github_author_is_site_admin) AS is_site_admin,

        -- Commit counts
        COUNT(*) AS total_commits,
        COUNT(DISTINCT repo) AS repos_contributed_to,
        COUNT(DISTINCT commit_date) AS active_days,

        -- Time-based metrics
        MIN(commit_timestamp) AS first_commit_at,
        MAX(commit_timestamp) AS last_commit_at,
        TIMESTAMP_DIFF(MAX(commit_timestamp), MIN(commit_timestamp), DAY) AS days_active_span,

        -- Commit type breakdown
        COUNTIF(commit_type = 'fix') AS fix_commits,
        COUNTIF(commit_type = 'feature') AS feature_commits,
        COUNTIF(commit_type = 'docs') AS docs_commits,
        COUNTIF(commit_type = 'refactor') AS refactor_commits,
        COUNTIF(commit_type = 'test') AS test_commits,
        COUNTIF(commit_type = 'chore') AS chore_commits,
        COUNTIF(commit_type = 'performance') AS performance_commits,
        COUNTIF(commit_type = 'other') AS other_commits,

        -- Verification metrics
        COUNTIF(is_verified) AS verified_commits,
        COUNTIF(NOT is_verified) AS unverified_commits,

        -- Merge commits
        COUNTIF(is_merge_commit) AS merge_commits,

        -- PR-related commits
        COUNTIF(pull_request_number IS NOT NULL) AS pr_commits,
        COUNT(DISTINCT pull_request_number) AS unique_prs

    FROM commits
    WHERE github_author_id IS NOT NULL
    GROUP BY
        github_author_id,
        github_author_login,
        github_author_avatar_url,
        github_author_profile_url,
        github_author_type,
        git_author_name,
        git_author_email
),

recent_activity AS (
    SELECT
        github_author_id,
        git_author_email,

        -- Recent activity metrics (last 30, 90, 180, 365 days)
        COUNTIF(commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY))
            AS commits_last_30_days,
        COUNTIF(commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY))
            AS commits_last_90_days,
        COUNTIF(commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY))
            AS commits_last_180_days,
        COUNTIF(commit_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY))
            AS commits_last_365_days,

        -- Active days in recent periods
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

        -- Most recent commit details
        MAX(commit_timestamp) AS most_recent_commit_at

    FROM commits
    WHERE github_author_id IS NOT NULL
    GROUP BY github_author_id, git_author_email
),

time_pattern_counts AS (
    SELECT
        github_author_id,
        git_author_email,

        -- Commit patterns by day of week (1=Sunday, 7=Saturday)
        COUNTIF(commit_day_of_week IN (1, 7)) AS weekend_commits,
        COUNTIF(commit_day_of_week BETWEEN 2 AND 6) AS weekday_commits,

        -- Commit patterns by hour (working hours 9-17)
        COUNTIF(commit_hour BETWEEN 9 AND 17) AS business_hours_commits,
        COUNTIF(commit_hour < 9 OR commit_hour > 17) AS off_hours_commits

    FROM commits
    WHERE github_author_id IS NOT NULL
    GROUP BY github_author_id, git_author_email
),

hour_counts AS (
    SELECT
        github_author_id,
        git_author_email,
        commit_hour,
        COUNT(*) AS commit_hour_count
    FROM commits
    WHERE github_author_id IS NOT NULL
    GROUP BY github_author_id, git_author_email, commit_hour
),

hour_mode AS (
    SELECT
        github_author_id,
        git_author_email,
        commit_hour AS most_active_hour
    FROM hour_counts
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY github_author_id, git_author_email
        ORDER BY commit_hour_count DESC, commit_hour
    ) = 1
),

dow_counts AS (
    SELECT
        github_author_id,
        git_author_email,
        commit_day_of_week,
        COUNT(*) AS commit_dow_count
    FROM commits
    WHERE github_author_id IS NOT NULL
    GROUP BY github_author_id, git_author_email, commit_day_of_week
),

dow_mode AS (
    SELECT
        github_author_id,
        git_author_email,
        commit_day_of_week AS most_active_day_of_week
    FROM dow_counts
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY github_author_id, git_author_email
        ORDER BY commit_dow_count DESC, commit_day_of_week
    ) = 1
),

time_patterns AS (
    SELECT
        counts.github_author_id,
        counts.git_author_email,
        counts.weekend_commits,
        counts.weekday_commits,
        counts.business_hours_commits,
        counts.off_hours_commits,
        hour_mode.most_active_hour,
        dow_mode.most_active_day_of_week
    FROM time_pattern_counts AS counts
    LEFT JOIN hour_mode
        ON
            counts.github_author_id = hour_mode.github_author_id
            AND counts.git_author_email = hour_mode.git_author_email
    LEFT JOIN dow_mode
        ON
            counts.github_author_id = dow_mode.github_author_id
            AND counts.git_author_email = dow_mode.git_author_email
),

contribution_metrics AS (
    SELECT
        github_author_id,
        git_author_email,

        -- Calculate average commits per active period
        AVG(commits_per_day) AS avg_commits_per_active_day,
        MAX(commits_per_day) AS max_commits_in_day

    FROM (
        SELECT
            github_author_id,
            git_author_email,
            commit_date,
            COUNT(*) AS commits_per_day
        FROM commits
        WHERE github_author_id IS NOT NULL
        GROUP BY github_author_id, git_author_email, commit_date
    )
    GROUP BY github_author_id, git_author_email
),

final AS (
    SELECT
        -- Committer Identity
        base.github_author_id AS committer_id,
        base.github_author_login AS github_login,
        base.git_author_name AS committer_name,
        base.git_author_email AS committer_email,

        -- GitHub Profile
        base.github_author_avatar_url AS avatar_url,
        base.github_author_profile_url AS profile_url,
        base.github_author_type AS account_type,
        base.is_site_admin,

        -- Overall Contribution Stats
        base.total_commits,
        base.repos_contributed_to,
        base.active_days,
        base.days_active_span,

        -- First & Last Activity
        base.first_commit_at,
        base.last_commit_at,
        base.fix_commits,

        -- Activity Status
        base.feature_commits,

        -- Commit Type Breakdown
        base.docs_commits,
        base.refactor_commits,
        base.test_commits,
        base.chore_commits,
        base.performance_commits,
        base.other_commits,
        base.verified_commits,
        base.unverified_commits,

        -- Commit Type Percentages
        base.merge_commits,
        base.pr_commits,
        base.unique_prs,
        recent.commits_last_30_days,

        -- Verification Stats
        recent.commits_last_90_days,
        recent.commits_last_180_days,
        recent.commits_last_365_days,

        -- Merge & PR Stats
        recent.active_days_last_30,
        recent.active_days_last_90,
        recent.active_days_last_365,
        time_p.weekend_commits,

        -- Recent Activity Metrics
        time_p.weekday_commits,
        time_p.business_hours_commits,
        time_p.off_hours_commits,
        time_p.most_active_hour,
        time_p.most_active_day_of_week,
        contrib.max_commits_in_day,
        TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), base.last_commit_at, DAY) AS days_since_last_commit,

        -- Time Patterns
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

        -- Productivity Metrics
        ROUND(100.0 * SAFE_DIVIDE(base.merge_commits, base.total_commits), 2) AS merge_commit_pct,
        ROUND(contrib.avg_commits_per_active_day, 2) AS avg_commits_per_active_day,
        ROUND(SAFE_DIVIDE(base.total_commits, base.active_days), 2) AS commits_per_active_day,
        ROUND(SAFE_DIVIDE(base.total_commits, GREATEST(base.days_active_span, 1)), 2)
            AS avg_commits_per_day_in_span,

        -- Ranking Metrics
        ROW_NUMBER() OVER (ORDER BY base.total_commits DESC) AS rank_by_total_commits,
        ROW_NUMBER() OVER (ORDER BY recent.commits_last_90_days DESC) AS rank_by_recent_activity,

        -- Metadata
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM committer_base_stats AS base
    LEFT JOIN recent_activity AS recent
        ON
            base.github_author_id = recent.github_author_id
            AND base.git_author_email = recent.git_author_email
    LEFT JOIN time_patterns AS time_p
        ON
            base.github_author_id = time_p.github_author_id
            AND base.git_author_email = time_p.git_author_email
    LEFT JOIN contribution_metrics AS contrib
        ON
            base.github_author_id = contrib.github_author_id
            AND base.git_author_email = contrib.git_author_email
)

SELECT * FROM final
ORDER BY total_commits DESC
