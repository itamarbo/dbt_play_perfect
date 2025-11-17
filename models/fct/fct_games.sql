-- {{
--   config(
--     materialized='incremental',
--     unique_key=['player_id', 'room_id'],
--     partition_by={
--       "field": "date_utc", 
--       "data_type": "date"
--     },
--     cluster_by=['player_id', 'room_id'],
--     tags=['hourly', 'fct']
--   )
-- }}

-- -- Select statement to aggregate event data into a game-level fact table (player x room).
-- -- The result summarizes all activity of a single player within a single tournament room.
-- WITH player_events AS (
--   SELECT
--     player_id, 
--     room_id,
--     date_utc,
--     timestamp_utc, 
    
--     -- Measure 1: Total coins spent for entry (uses 'coins_spent' column)
--     -- ABS is used to ensure the value is positive, as 'coins_spent' might be negative in the source.
--     SUM(CASE WHEN event_name = 'tournament_entry' THEN ABS(coins_spent) ELSE 0 END) AS entry_coins_spent,
    
--     -- Measure 2: Total coins rewarded (uses 'reward' column)
--     SUM(CASE WHEN event_name = 'reward_coins' THEN reward ELSE 0 END) AS total_reward_coins,
    
--     -- Measure 3: Count of critical actions (e.g., moves or item usage)
--     -- Using PostgreSQL conditional aggregation syntax
--     COUNT(CASE WHEN event_name IN ('move_made', 'item_used') THEN 1 END) AS critical_actions_count,
    
--     -- Measure 4: Tracking the final rank (uses 'position' column)
--     MAX(CASE WHEN event_name = 'match_completed' THEN position ELSE NULL END) AS final_rank,
    
--     -- Time tracking for match duration and incremental logic
--     MIN(timestamp_utc) AS match_start_time,
--     MAX(timestamp_utc) AS last_event_time -- Used for incremental comparison
    
--   FROM 
--     {{ source('games', 'events') }} 

--   -- Optimization for Cost (PostgreSQL Date Logic and Incremental Logic)
--   WHERE 
--     room_id IS NOT NULL 
--     AND player_id IS NOT NULL
    
--     -- 1. FIX: Explicitly cast 'date_utc' column to DATE for comparison (Robustness/Data Integrity)
--     AND date_utc::date >= CURRENT_DATE - INTERVAL '2 day' 

--     {% if is_incremental() %}
--       -- 2. Use the timestamp to only process new events since the last run
--       -- This ensures we only scan events newer than the max time recorded in the target table.
--       AND timestamp_utc > (SELECT MAX(last_event_time) FROM {{ this }})
--     {% endif %}

--   GROUP BY 1, 2, 3, 4
-- )

-- SELECT
--   player_id,
--   room_id,
--   last_event_time::date AS date_utc, -- PostgreSQL date conversion (Partition Key)
--   entry_coins_spent,
--   total_reward_coins,
--   critical_actions_count,
--   final_rank,
--   match_start_time,
--   last_event_time
-- FROM
--   player_events
-- WHERE
--   room_id IS NOT NULL 
--   AND player_id IS NOT NULL 
--   AND match_start_time IS NOT NULL