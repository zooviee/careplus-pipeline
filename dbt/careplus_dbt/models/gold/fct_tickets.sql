WITH tickets AS (
    SELECT * FROM {{ ref('stg_tickets') }}
)

SELECT
    ticket_id,
    created_at,
    resolved_at,
    agent,
    priority,
    num_interactions,
    issue_cat,
    channel,
    status,
    agent_feedback,
    resolution_time_mins,
    is_open,

    -- Derived metrics
    DATE(created_at)                                AS created_date,
    DAYNAME(created_at)                             AS created_day_of_week,
    HOUR(created_at)                                AS created_hour,

    CASE
        WHEN resolution_time_mins < 60   THEN 'Under 1 hour'
        WHEN resolution_time_mins < 1440 THEN 'Under 1 day'
        WHEN resolution_time_mins < 4320 THEN 'Under 3 days'
        ELSE 'Over 3 days'
    END                                             AS resolution_bucket

FROM tickets