WITH source AS (
    SELECT * FROM {{ source('bronze', 'logs') }}
),

cleaned AS (
    SELECT
        timestamp,

        -- Normalize dirty log levels
        CASE UPPER(TRIM(log_level))
            WHEN 'INFO'    THEN 'INFO'
            WHEN 'INF0'    THEN 'INFO'
            WHEN 'DEBUG'   THEN 'DEBUG'
            WHEN 'DEBG'    THEN 'DEBUG'
            WHEN 'ERROR'   THEN 'ERROR'
            WHEN 'EROR'    THEN 'ERROR'
            WHEN 'WarnING' THEN 'WARNING'
            WHEN 'WARNING' THEN 'WARNING'
            ELSE UPPER(TRIM(log_level))
        END                                         AS log_level,

        service,
        ticket_id,
        session_id,
        ip,

        -- Null out negative response times
        CASE
            WHEN response_time_ms < 0 THEN NULL
            ELSE response_time_ms
        END                                         AS response_time_ms,

        cpu_percent,
        event_type,
        CASE LOWER(TRIM(error))
            WHEN 'true'  THEN TRUE
            WHEN 'false' THEN FALSE
            ELSE NULL
        END                                         AS error,
        user_agent,
        trace_id

    FROM source

    -- Remove duplicates
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY timestamp, ticket_id, ip
        ORDER BY timestamp
    ) = 1
)

SELECT * FROM cleaned