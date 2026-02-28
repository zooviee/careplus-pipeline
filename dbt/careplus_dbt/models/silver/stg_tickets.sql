WITH source AS (
    SELECT * FROM {{ source('bronze', 'tickets') }}
),

cleaned AS (
    SELECT
        ticket_id,
        created_at,
        resolved_at,
        TRIM(agent)                                         AS agent,

        -- Normalize dirty priority values
        CASE UPPER(TRIM(priority))
            WHEN 'HIGH'   THEN 'High'
            WHEN 'HGH'    THEN 'High'
            WHEN 'MEDIUM' THEN 'Medium'
            WHEN 'MEDUM'    THEN 'Medium'
            WHEN 'LOW'    THEN 'Low'
            WHEN 'LW'     THEN 'Low'
            ELSE TRIM(priority)
        END                                                 AS priority,

        -- Null out invalid num_interactions
        CASE
            WHEN num_interactions < 0 THEN NULL
            ELSE num_interactions
        END                                                 AS num_interactions,

        issue_cat,
        channel,
        status,
        agent_feedback,

        -- Derived columns
        DATEDIFF('minute', created_at, resolved_at)         AS resolution_time_mins,
        CASE WHEN resolved_at IS NULL THEN TRUE ELSE FALSE END AS is_open

    FROM source
)

SELECT * FROM cleaned