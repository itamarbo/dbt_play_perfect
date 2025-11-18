{{ config(
    materialized='incremental',
    unique_key=['player_id', 'room_id', 'joined_time', 'submit_time', 'room_close_time', 'claim_time'],
    partition_by={
      "field": "date_utc",
      "data_type": "date"
    },
    cluster_by=['player_id', 'tournament_id'],
    hours_to_refresh=1,
    incremental_strategy='append'
) }}

-- Fact CTE: finding all the main event times and calculating balances 
with player_match_events as (
    select
        date_utc,
        player_id,
        tournament_id,
        room_id,
        -- Event times: taking the appropriate timestamp for each action
        max(case when event_name = 'tournamentJoined' then timestamp_utc end) as joined_time,
        max(case when event_name = 'tournamentFinished' then timestamp_utc end) as submit_time,
        max(case when event_name = 'tournamentRoomClosed' then timestamp_utc end) as room_close_time,
        max(case when event_name = 'tournamentRewardClaimed' then timestamp_utc end) as claim_time,

        -- Static game data
        max(entry_fee) as entry_fee,
        max(players_capacity) as players_capacity,
        max(play_duration) as play_duration,
        max(score) as score,
        max("position") as "position",
        max(reward) as reward,

        -- Balances and costs: taking the value before and after the events
        max(case when event_name = 'tournamentJoined' then balance_before end) as balance_before,
        max(case when event_name = 'tournamentRewardClaimed' then balance_before + coins_claimed end) as balance_after_claim,

        -- Did the player claim a reward?
        max(case when event_name = 'tournamentRewardClaimed' then 1 else 0 end) as did_claim_reward
    
    from {{ ref('stg_events') }}
    where event_name in ('tournamentJoined', 'tournamentFinished', 'tournamentRoomClosed', 'tournamentRewardClaimed')
    group by date_utc, player_id, tournament_id, room_id
),

-- CTE: finding the actual number of players in the room
actual_players as (
    select
        room_id,
        count(distinct player_id) as actual_players_in_room
    from {{ ref('stg_events') }}
    where event_name = 'tournamentJoined'
    group by room_id
)

select
    t1.date_utc,
    t1.player_id,
    t1.joined_time,
    t1.submit_time,
    t1.room_close_time,
    t1.play_duration,
    t1.balance_before,
    -- COALESCE: If there is no award claim, the balance remains as the balance before the claim (which is NULL here if there was no claim)
    coalesce(t1.balance_after_claim, t1.balance_before - t1.entry_fee) as balance_after_claim,
    t1.tournament_id,
    t1.room_id,
    t1.entry_fee,
    t1.players_capacity,
    t2.actual_players_in_room,
    t1.score,
    t1."position",
    t1.reward,
    t1.did_claim_reward,
    t1.claim_time,
    CURRENT_TIMESTAMP as fct_created_date
from player_match_events t1
left join actual_players t2 on t1.room_id = t2.room_id
-- insure we only get records where the player actually joined the match
where t1.joined_time is not null
{% if is_incremental() %}
  and t1.date_utc >= (select max(date_utc) from {{ this }}) -- load from the last loaded date
{% endif %}