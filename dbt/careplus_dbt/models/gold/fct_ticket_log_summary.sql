WITH tickets AS (
    SELECT * FROM {{ ref('fct_tickets') }}
),

logs AS (
    SELECT * FROM {{ ref('fct_logs') }}
),

log_metrics AS (
    SELECT
        ticket_id,
        COUNT(*)                                    AS total_log_events,
        AVG(response_time_ms)                       AS avg_response_time_ms,
        MAX(response_time_ms)                       AS max_response_time_ms,
        AVG(cpu_percent)                            AS avg_cpu_percent,
        SUM(CASE WHEN error = TRUE THEN 1 ELSE 0 END) AS total_errors,
        SUM(CASE WHEN log_level = 'WARNING' THEN 1 ELSE 0 END) AS total_warnings,
        SUM(CASE WHEN log_level = 'DEBUG' THEN 1 ELSE 0 END)   AS total_debug_events,
        MIN(timestamp)                              AS first_log_at,
        MAX(timestamp)                              AS last_log_at
    FROM logs
    GROUP BY ticket_id
)

SELECT
    t.ticket_id,
    t.created_at,
    t.resolved_at,
    t.agent,
    t.priority,
    t.issue_cat,
    t.channel,
    t.status,
    t.num_interactions,
    t.resolution_time_mins,
    t.resolution_bucket,
    t.is_open,
    t.created_date,
    t.created_day_of_week,

    -- Log metrics
    l.total_log_events,
    l.avg_response_time_ms,
    l.max_response_time_ms,
    l.avg_cpu_percent,
    l.total_errors,
    l.total_warnings,
    l.total_debug_events,
    l.first_log_at,
    l.last_log_at

FROM tickets t
LEFT JOIN log_metrics l ON t.ticket_id = l.ticket_id