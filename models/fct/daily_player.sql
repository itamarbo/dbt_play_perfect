{{ config(
    materialized='incremental',
    unique_key=['date_utc', 'player_id'],
    partition_by={
      "field": "date_utc",
      "data_type": "date"
    },
    cluster_by=['player_id'],
    incremental_strategy='merge'
) }}

-- Daily player activity summary: one row per player per day
with player_daily_events as (
    select
        date_utc,
        player_id,
        timestamp_utc,
        event_name,
        balance_before,
        entry_fee,
        coins_spent,
        coins_claimed,
        reward,
        play_duration,
        score,
        position,
        price_usd,
        room_id
    from {{ ref('stg_events') }}
),

player_daily_summary as (
    select
        date_utc,
        player_id,
        
        -- Balance: opening balance (first event) and closing balance (last event) for the day
        (array_agg(balance_before order by timestamp_utc asc))[1] as balance_day_start,
        (array_agg(balance_before order by timestamp_utc desc))[1] as balance_day_end,

        -- Match statistics
        count(distinct case when event_name = 'tournamentJoined' then room_id end) as matches_played,
        sum(case when event_name = 'tournamentFinished' then play_duration else 0 end) as total_matches_duration,
        count(distinct case when event_name = 'tournamentRewardClaimed' and reward > 0 then room_id end) as matches_won_reward,
        count(distinct case when event_name = 'tournamentRewardClaimed' then room_id end) as matches_claimed,

        -- Coins flow
        sum(case when event_name = 'tournamentJoined' then entry_fee else 0 end) as coins_sink_tournaments,
        sum(case when event_name = 'tournamentRewardClaimed' then coins_claimed else 0 end) as coins_source_tournaments,

        -- Performance metrics
        max(score) as max_score,
        avg(case when score is not null then score end) as avg_score,
        min(position) as max_position, -- Best position (lowest number = best rank)
        avg(case when position is not null then position end) as avg_position,

        -- Purchase activity
        sum(case when event_name = 'purchase' then price_usd else 0 end) as revenue,
        sum(case when event_name = 'purchase' then coins_spent else 0 end) as coins_source_purchases

    from player_daily_events
    group by date_utc, player_id
)

select
    date_utc,
    player_id,
    balance_day_start,
    balance_day_end,
    matches_played,
    total_matches_duration,
    matches_won_reward,
    matches_claimed,
    coins_sink_tournaments,
    coins_source_tournaments,
    max_score,
    avg_score,
    max_position,
    avg_position,
    
    -- Streaks - these require window functions over time, placeholder for now
    0 as max_reward_won_streak,
    0 as max_losing_streak,
    
    revenue,
    coins_source_purchases

from player_daily_summary

{% if is_incremental() %}
where date_utc >= (select max(date_utc) from {{ this }})
{% endif %}
