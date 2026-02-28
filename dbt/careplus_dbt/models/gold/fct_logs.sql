WITH logs AS (
    SELECT * FROM {{ ref('stg_logs') }}
)

SELECT
    timestamp,
    log_level,
    service,
    ticket_id,
    session_id,
    ip,
    response_time_ms,
    cpu_percent,
    event_type,
    error,
    user_agent,
    trace_id,

    -- Derived metrics
    DATE(timestamp)                                 AS log_date,
    HOUR(timestamp)                                 AS log_hour,

    CASE
        WHEN response_time_ms < 500  THEN 'Fast'
        WHEN response_time_ms < 1000 THEN 'Normal'
        WHEN response_time_ms >= 1000 THEN 'Slow'
        ELSE 'Unknown'
    END                                             AS response_bucket,

    CASE
        WHEN cpu_percent >= 80 THEN 'High'
        WHEN cpu_percent >= 50 THEN 'Medium'
        ELSE 'Low'
    END                                             AS cpu_load

FROM logs