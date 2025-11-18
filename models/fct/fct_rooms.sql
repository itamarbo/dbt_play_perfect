{{ config(
    materialized='incremental',
    unique_key='room_id',
    cluster_by=['tournament_id', 'room_id'],
    incremental_strategy='merge'
) }}

-- Room summary: one row per room
with room_events as (
    select
        room_id,
        tournament_id,
        timestamp_utc,
        event_name,
        player_id,
        players_capacity,
        entry_fee,
        play_duration,
        reward
    from {{ ref('stg_events') }}
    where room_id is not null
),

room_summary as (
    select
        room_id,
        max(tournament_id) as tournament_id,
        max(players_capacity) as players_capacity,
        max(entry_fee) as entry_fee,
        
        -- Room timing
        min(case when event_name = 'tournamentJoined' then timestamp_utc end) as room_open_time,
        max(case when event_name = 'tournamentRoomClosed' then timestamp_utc end) as room_closing_time,
        
        -- Player counts
        count(distinct case when event_name = 'tournamentJoined' then player_id end) as actual_players,
        
        -- Coins economics
        sum(case when event_name = 'tournamentJoined' then entry_fee else 0 end) as total_coins_sink,
        sum(case when event_name = 'tournamentRewardClaimed' then reward else 0 end) as total_coins_rewards,
        
        -- Duration metrics
        avg(case when event_name = 'tournamentFinished' and play_duration is not null then play_duration end) as avg_play_duration,
        
        -- Room status flags
        max(case when event_name = 'tournamentRoomClosed' then 1 else 0 end) as is_closed

    from room_events
    group by room_id
)

select
    room_id,
    tournament_id,
    players_capacity,
    entry_fee,
    room_open_time,
    room_closing_time,
    actual_players,
    total_coins_sink,
    total_coins_rewards,
    avg_play_duration,
    
    -- Calculate room open duration in minutes
    case 
        when room_closing_time is not null and room_open_time is not null 
        then extract(epoch from (room_closing_time - room_open_time)) / 60
        else null
    end as room_open_duration,
    
    is_closed,
    
    -- Check if room is full
    case 
        when actual_players >= players_capacity then 1 
        else 0 
    end as is_full

from room_summary

{% if is_incremental() %}
where room_open_time >= (select max(room_open_time) - interval '1 hour' from {{ this }})
{% endif %}
