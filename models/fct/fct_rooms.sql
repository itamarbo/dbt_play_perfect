{{
  config(
    materialized='incremental',
    unique_key='room_id',
    partition_by={
      "field": "room_end_date_utc", 
      "data_type": "date"
    },
    cluster_by=['room_id', 'level'],
    tags=['hourly', 'fct']
  )
}}

WITH room_aggregations AS (
  SELECT
    room_id,
    
    -- Identify the room's attributes from any event
    MAX(level) AS room_level, 
    MAX(players_capacity) AS max_players_limit, 
    
    -- Count distinct players who joined
    COUNT(DISTINCT player_id) AS total_players_in_room,
    
    -- Sum of all coins rewarded across all players in this room 
    SUM(CASE WHEN event_name = 'reward_coins' THEN reward ELSE 0 END) AS total_coins_rewarded,
    
    -- Tracking start and end times
   MIN(timestamp_utc::timestamp) AS room_start_timestamp, 
    MAX(CASE WHEN event_name = 'room_end' THEN timestamp_utc::timestamp ELSE NULL END) AS room_end_timestamp
    
  FROM 
    {{ source('games', 'events') }} 

  -- Optimization for Cost (PostgreSQL Date Logic and Incremental Logic)
  WHERE 
    room_id IS NOT NULL
    -- We scan the last 2 days (PostgreSQL interval subtraction)
    AND date_utc::date  >= CURRENT_DATE - INTERVAL '2 day' 

    {% if is_incremental() %}
      -- Retrieve the MAX room_start_timestamp from the existing table for incremental filtering
      AND timestamp_utc > (
        SELECT 
          MAX(room_start_timestamp)
        FROM 
          {{ this }}
      )
    {% endif %}

  GROUP BY 1
)

SELECT
  room_id,
  room_level AS level,
  total_players_in_room AS total_players,
  total_coins_rewarded,
  room_start_timestamp,
  room_end_timestamp,
  room_end_timestamp::date AS room_end_date_utc, -- PostgreSQL date conversion (Partition Key)
  
  -- Robustness check: room duration calculation in minutes (PostgreSQL)
  EXTRACT(EPOCH FROM (room_end_timestamp - room_start_timestamp)) / 60 AS room_duration_minutes,
  
  -- Data Integrity check: Has the room officially ended?
  CASE 
    WHEN room_end_timestamp IS NOT NULL THEN TRUE 
    ELSE FALSE 
  END AS is_room_completed

FROM
  room_aggregations
WHERE
  room_end_timestamp IS NOT NULL 
  AND room_id IS NOT NULL